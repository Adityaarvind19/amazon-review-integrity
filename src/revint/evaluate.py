"""Detector evaluation against injected campaigns (precision / recall / AUC / rating recovery)."""
from __future__ import annotations

import copy
import json
import statistics
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import pipeline
from .config import lake
from .inject import inject
from .silver import review_id_col
from .spark import get_spark, read_categories

DETECTOR_NAMES = ["near_dup", "copy_paste", "ring", "burst", "mismatch", "combined_flag"]


def _detectors() -> dict:
    return {"near_dup": F.col("x_near_dup") > 0, "copy_paste": F.col("x_copy_paste") > 0,
            "ring": F.col("x_ring") > 0, "burst": F.col("burst_z").isNotNull(),
            "mismatch": F.col("is_mismatch"), "combined_flag": F.col("is_flagged")}


def auc(scores: list[float], labels: list[int]) -> float | None:
    """Mann-Whitney AUC with average ranks for ties."""
    s = pd.Series(scores).rank(method="average")
    pos = sum(labels)
    neg = len(labels) - pos
    if pos == 0 or neg == 0:
        return None
    r_pos = s[[bool(x) for x in labels]].sum()
    return float((r_pos - pos * (pos + 1) / 2) / (pos * neg))


def evaluate(spark: SparkSession, cfg: dict, category: str, labels_path: str, targets_path: str) -> dict:
    labels = (spark.read.json(labels_path).withColumn("review_id", review_id_col())
              .select("review_id", "campaign_type", "campaign_id", "is_target_review").dropDuplicates(["review_id"]))
    fact = read_categories(spark, lake(cfg, "gold", "fact_review"), [category])
    j = fact.join(labels, "review_id", "left").withColumn("injected", F.col("campaign_type").isNotNull()).cache()
    n_labels = labels.count()
    matched = j.filter("injected").count()

    tgt = j.filter(F.col("is_target_review") == True)  # noqa: E712
    per_type = {}
    exprs = [F.count("*").alias("n")] + [F.avg(c.cast("double")).alias(k) for k, c in _detectors().items()]
    for r in tgt.groupBy("campaign_type").agg(*exprs).collect():
        per_type[r["campaign_type"]] = {k: (round(r[k], 4) if k != "n" else r[k]) for k in ["n", *DETECTOR_NAMES]}
    overall = tgt.agg(*exprs).first().asDict()

    flagged = j.filter("is_flagged")
    n_flagged = flagged.count()
    n_flag_inj = flagged.filter("injected").count()
    targets = json.loads(Path(targets_path).read_text())
    tdf = spark.createDataFrame([(k, v) for k, v in targets.items()], "parent_asin string, campaign_id string")
    dim = read_categories(spark, lake(cfg, "gold", "dim_product"), [category]).select("product_key", "parent_asin")
    tkeys = tdf.join(dim, "parent_asin").select("product_key")
    on_t = flagged.join(tkeys, "product_key")
    n_on_t = on_t.count()
    n_on_t_inj = on_t.filter("injected").count()
    organic = j.filter(~F.col("injected"))
    org_n = organic.count()
    org_flag = organic.filter("is_flagged").count()

    # product level
    pm = read_categories(spark, lake(cfg, "gold", "mart_product_integrity"), [category]) \
        .select("product_key", "parent_asin", "n_reviews", "integrity_score", "raw_avg_rating", "trusted_mean_rating")
    org_rating = organic.groupBy("product_key").agg(F.avg("rating").alias("organic_rating"),
                                                    F.count("*").alias("n_organic"))
    pdf = (pm.filter("n_reviews >= 5").join(org_rating, "product_key", "left")
           .join(tdf.withColumnRenamed("campaign_id", "target_campaign"), "parent_asin", "left").toPandas())
    pdf["is_target"] = pdf["target_campaign"].notna().astype(int)
    pdf["ctype"] = pdf["target_campaign"].str.rsplit("_", n=2).str[0]
    pdf["risk"] = 100 - pdf["integrity_score"]
    k = int(pdf["is_target"].sum())
    top = pdf.sort_values(["risk", "n_reviews"], ascending=[False, False]).head(k)
    prod = {"products_evaluated": len(pdf), "target_products": k,
            "auc_integrity_score": auc(pdf["risk"].tolist(), pdf["is_target"].tolist()),
            "precision_at_k": float(top["is_target"].mean()) if k else None,
            "recall_at_k": float(top["is_target"].sum() / k) if k else None,
            "recall_at_k_by_type": {t: float(top[top.ctype == t].shape[0] / max(1, (pdf.ctype == t).sum()))
                                    for t in sorted(pdf.ctype.dropna().unique())}}
    t = pdf[(pdf.is_target == 1) & pdf.organic_rating.notna()]
    raw_err = (t.raw_avg_rating - t.organic_rating).abs()
    tr_err = (t.trusted_mean_rating.fillna(t.raw_avg_rating) - t.organic_rating).abs()
    rating = {"target_products_with_organic_reviews": len(t),
              "mean_abs_error_raw_vs_organic": float(raw_err.mean()) if len(t) else None,
              "mean_abs_error_trusted_vs_organic": float(tr_err.mean()) if len(t) else None}
    if len(t) and raw_err.mean() > 0:
        rating["rating_distortion_removed"] = float(1 - tr_err.mean() / raw_err.mean())
    j.unpersist()
    return {
        "category": category, "labels": n_labels, "labels_matched_in_fact": matched,
        "review_level": {
            "target_reviews": overall["n"],
            "recall_combined": round(overall["combined_flag"], 4),
            "recall_by_detector": {kk: round(overall[kk], 4) for kk in DETECTOR_NAMES if kk != "combined_flag"},
            "recall_by_campaign_type": per_type,
            "flagged_reviews": n_flagged, "flagged_injected": n_flag_inj,
            "precision_lower_bound": n_flag_inj / n_flagged if n_flagged else None,
            "precision_on_target_products": n_on_t_inj / n_on_t if n_on_t else None,
            "organic_reviews": org_n, "organic_flag_rate": org_flag / org_n if org_n else None},
        "product_level": prod, "rating_recovery": rating}


def _mean_std(vals):
    vals = [v for v in vals if v is not None]
    if not vals:
        return None
    return {"mean": round(statistics.mean(vals), 4), "std": round(statistics.pstdev(vals), 4), "n": len(vals)}


def run_eval(cfg: dict, category: str, seeds: list[int], campaigns_per_type: int, work_dir: str,
             publish: bool = True) -> dict:
    results = []
    for seed in seeds:
        base = Path(work_dir, f"seed={seed}")
        inj = inject(cfg["paths"]["raw_dir"], str(base / "raw"), category, seed, campaigns_per_type)
        c = copy.deepcopy(cfg)
        c["paths"].update(raw_dir=str(base / "raw"), lake_dir=str(base / "lake"), state_dir=str(base / "state"))
        man = pipeline.run(c, [category], force=True, publish=False, tag=f"eval_seed{seed}")
        spark = get_spark(c)
        res = evaluate(spark, c, category, inj["labels"], inj["targets"])
        res.update(seed=seed, injection=inj, pipeline_seconds=man["total_seconds"])
        results.append(res)
        print(f"[eval] seed={seed} recall={res['review_level']['recall_combined']} "
              f"auc={res['product_level']['auc_integrity_score']}")
    g = lambda f: _mean_std([f(r) for r in results])  # noqa: E731
    agg = {
        "recall_combined": g(lambda r: r["review_level"]["recall_combined"]),
        "precision_lower_bound": g(lambda r: r["review_level"]["precision_lower_bound"]),
        "precision_on_target_products": g(lambda r: r["review_level"]["precision_on_target_products"]),
        "organic_flag_rate": g(lambda r: r["review_level"]["organic_flag_rate"]),
        "product_auc": g(lambda r: r["product_level"]["auc_integrity_score"]),
        "product_precision_at_k": g(lambda r: r["product_level"]["precision_at_k"]),
        "product_recall_at_k": g(lambda r: r["product_level"]["recall_at_k"]),
        "rating_distortion_removed": g(lambda r: r["rating_recovery"].get("rating_distortion_removed")),
        "recall_by_campaign_type": {
            t: g(lambda r, t=t: r["review_level"]["recall_by_campaign_type"].get(t, {}).get("combined_flag"))
            for t in ["TEMPLATE", "REVIEW_BOMB", "RING", "BURST", "SLOW_DRIP"]},
        "recall_by_detector": {
            d: g(lambda r, d=d: r["review_level"]["recall_by_detector"][d])
            for d in ["near_dup", "copy_paste", "ring", "burst", "mismatch"]},
    }
    report = {"category": category, "seeds": seeds, "campaigns_per_type": campaigns_per_type,
              "aggregate": agg, "per_seed": results}
    if publish:
        out = Path(cfg["paths"]["results_dir"], "eval")
        out.mkdir(parents=True, exist_ok=True)
        (out / "eval_report.json").write_text(json.dumps(report, indent=2, default=str))
        (out / "eval_report.md").write_text(to_markdown(report))
    return report


def _f(x, pct=True):
    if x is None:
        return "n/a"
    return f"{x['mean'] * 100:.1f}% ± {x['std'] * 100:.1f}" if pct else f"{x['mean']:.3f} ± {x['std']:.3f}"


def to_markdown(rep: dict) -> str:
    a = rep["aggregate"]
    lines = [f"# Detector evaluation — {rep['category']}",
             "",
             f"Seeds: {rep['seeds']} · campaigns per type per seed: {rep['campaigns_per_type']} · "
             "mean ± std across seeds. Synthetic campaigns injected into the real raw file; full pipeline rerun per seed.",
             "",
             "| Metric | Value |", "|---|---|",
             f"| Review recall (combined flag) | {_f(a['recall_combined'])} |",
             f"| Precision, lower bound (every flagged organic review counted as FP) | {_f(a['precision_lower_bound'])} |",
             f"| Precision on targeted products | {_f(a['precision_on_target_products'])} |",
             f"| Organic review flag rate | {_f(a['organic_flag_rate'])} |",
             f"| Product AUC (100 − integrity score) | {_f(a['product_auc'], pct=False)} |",
             f"| Product precision@K / recall@K (K = #targets) | {_f(a['product_precision_at_k'])} / {_f(a['product_recall_at_k'])} |",
             f"| Rating distortion removed by trust-adjusted rating | {_f(a['rating_distortion_removed'])} |",
             "", "## Recall by campaign type", "", "| Type | Recall |", "|---|---|"]
    lines += [f"| {t} | {_f(v)} |" for t, v in a["recall_by_campaign_type"].items()]
    lines += ["", "## Recall by single detector (target reviews)", "", "| Detector | Recall |", "|---|---|"]
    lines += [f"| {d} | {_f(v)} |" for d, v in a["recall_by_detector"].items()]
    lines += ["", "Caveat: organic reviews are unlabeled, so real fakes in the data count as false positives here; "
              "precision is a lower bound. SLOW_DRIP is a deliberately hard case."]
    return "\n".join(lines) + "\n"
