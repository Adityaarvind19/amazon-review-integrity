"""Data-quality gate. `error` checks fail the run; `warn` checks are reported only."""
from __future__ import annotations

import math
from dataclasses import asdict, dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import lake
from .spark import read_categories, read_categories_or_none


@dataclass
class Check:
    name: str
    severity: str
    passed: bool
    observed: object
    expected: str


class DataQualityError(RuntimeError):
    pass


def _c(name, sev, passed, observed, expected):
    return Check(name, sev, bool(passed), observed, expected)


def silver_checks(spark: SparkSession, cfg: dict, categories: list[str], ingest_files: dict,
                  silver_stats: dict) -> list[Check]:
    out = []
    for c in categories:
        files = ingest_files[c]["files"]
        rev = next(v for k, v in files.items() if not k.startswith("meta_"))
        out.append(_c(f"{c}.bronze_reconciles_source_lines", "error",
                      rev["bronze_rows"] == rev["source_lines"],
                      {"bronze": rev["bronze_rows"], "source_lines": rev["source_lines"]}, "bronze == source lines"))
        s = silver_stats[c]
        total = s["silver_rows"] + s["quarantined"] + s["duplicates_removed"]
        out.append(_c(f"{c}.silver_reconciles_bronze", "error", total == s["bronze_rows"],
                      {"silver+quarantine+dupes": total, "bronze": s["bronze_rows"]}, "equal"))
        rate = s["quarantined"] / max(1, s["bronze_rows"])
        out.append(_c(f"{c}.quarantine_rate", "warn", rate <= 0.01, round(rate, 6), "<= 1%"))
    sv = read_categories(spark, lake(cfg, "silver", "reviews"), categories)
    agg = sv.agg(F.count("*").alias("n"), F.countDistinct("category", "review_id").alias("ids"),
                 F.sum((~F.col("rating").isin(1.0, 2.0, 3.0, 4.0, 5.0)).cast("int")).alias("bad_rating"),
                 F.sum((F.col("user_id").isNull() | F.col("parent_asin").isNull() | F.col("review_ts").isNull())
                       .cast("int")).alias("null_keys"),
                 F.min("review_ts").alias("min_ts"), F.max("review_ts").alias("max_ts")).first()
    out.append(_c("silver.review_id_unique", "error", agg["n"] == agg["ids"], {"rows": agg["n"], "distinct": agg["ids"]}, "equal"))
    out.append(_c("silver.rating_domain", "error", agg["bad_rating"] == 0, agg["bad_rating"], "0 outside {1..5}"))
    out.append(_c("silver.not_null_keys", "error", agg["null_keys"] == 0, agg["null_keys"], "0"))
    out.append(_c("silver.timestamp_range", "error", agg["min_ts"] is not None and agg["min_ts"].year >= 1995,
                  f"{agg['min_ts']} .. {agg['max_ts']}", ">= 1995 and <= now"))
    prods = read_categories(spark, lake(cfg, "silver", "products"), categories)
    orphans = sv.select("category", "parent_asin").distinct().join(
        prods.select("category", "parent_asin"), ["category", "parent_asin"], "left_anti").count()
    n_prod = sv.select("category", "parent_asin").distinct().count()
    out.append(_c("silver.product_metadata_coverage", "warn", orphans / max(1, n_prod) <= 0.05,
                  {"products_without_meta": orphans, "reviewed_products": n_prod}, "<= 5% missing"))
    dupk = prods.groupBy("category", "parent_asin").count().filter("count > 1").count()
    out.append(_c("silver.products_unique", "error", dupk == 0, dupk, "0 duplicate parent_asin"))
    return out


def gold_checks(spark: SparkSession, cfg: dict, categories: list[str]) -> list[Check]:
    out = []
    g = lambda t: read_categories(spark, lake(cfg, "gold", t), categories)  # noqa: E731
    fact = g("fact_review")
    sv_n = read_categories(spark, lake(cfg, "silver", "reviews"), categories).count()
    fact_n = fact.count()
    out.append(_c("gold.fact_rowcount_matches_silver", "error", fact_n == sv_n, {"fact": fact_n, "silver": sv_n}, "equal"))
    dim_date = spark.read.parquet(lake(cfg, "gold", "dim_date"))
    miss_date = fact.join(dim_date, "date_key", "left_anti").count()
    out.append(_c("gold.fk_fact_date", "error", miss_date == 0, miss_date, "0 orphans"))
    dim_rev = spark.read.parquet(lake(cfg, "gold", "dim_reviewer"))
    miss_rev = fact.select("reviewer_key").distinct().join(dim_rev, "reviewer_key", "left_anti").count()
    out.append(_c("gold.fk_fact_reviewer", "error", miss_rev == 0, miss_rev, "0 orphans"))
    dim_prod = g("dim_product")
    miss_prod = fact.select("category", "product_key").distinct().join(
        dim_prod, ["category", "product_key"], "left_anti").count()
    out.append(_c("gold.fk_fact_product", "error", miss_prod == 0, miss_prod, "0 orphans"))
    dpk = dim_prod.groupBy("category", "product_key").count().filter("count > 1").count()
    out.append(_c("gold.dim_product_key_unique", "error", dpk == 0, dpk, "0"))
    s = fact.agg(F.min("suspicion").alias("lo"), F.max("suspicion").alias("hi")).first()
    out.append(_c("gold.suspicion_in_unit_interval", "error",
                  s["lo"] is not None and 0 <= s["lo"] and s["hi"] <= 1, [s["lo"], s["hi"]], "[0, 1]"))
    pm = g("mart_product_integrity")
    r = pm.agg(F.min("integrity_score").alias("lo"), F.max("integrity_score").alias("hi"),
               F.min("trust_adjusted_rating").alias("tlo"), F.max("trust_adjusted_rating").alias("thi"),
               F.sum("n_reviews").alias("n")).first()
    out.append(_c("gold.integrity_score_range", "error", 0 <= r["lo"] and r["hi"] <= 100, [r["lo"], r["hi"]], "[0, 100]"))
    out.append(_c("gold.trusted_rating_range", "error", 1 <= r["tlo"] and r["thi"] <= 5, [r["tlo"], r["thi"]], "[1, 5]"))
    out.append(_c("gold.product_mart_reconciles_fact", "error", r["n"] == fact_n, {"mart": r["n"], "fact": fact_n}, "equal"))
    mm = g("mart_category_monthly").agg(F.sum("n_reviews")).first()[0]
    out.append(_c("gold.monthly_mart_reconciles_fact", "error", mm == fact_n, {"mart": mm, "fact": fact_n}, "equal"))
    feats = lake(cfg, "features")
    edges = read_categories_or_none(spark, f"{feats}/near_dup_edges", categories)
    bad_e = 0 if edges is None else edges.filter(F.col("jaccard") < cfg["near_dup"]["jaccard_threshold"]).count()
    out.append(_c("features.near_dup_edges_above_threshold", "error", bad_e == 0, bad_e, "0 below threshold"))
    rings = read_categories_or_none(spark, f"{feats}/rings", categories)
    bad_r = 0 if rings is None else rings.filter((F.col("n_members") < 2)).count()
    out.append(_c("features.rings_min_size", "error", bad_r == 0, bad_r, "0 rings with < 2 members"))
    bursts = read_categories_or_none(spark, f"{feats}/bursts", categories)
    bz = ({"z": None, "n": None} if bursts is None else
          bursts.agg(F.min("robust_z").alias("z"), F.min("n_reviews").alias("n")).first())
    ok = bz["z"] is None or (math.isfinite(bz["z"]) and bz["z"] >= cfg["burst"]["z_threshold"]
                             and bz["n"] >= cfg["burst"]["min_count"])
    out.append(_c("features.bursts_meet_thresholds", "error", ok, [bz["z"], bz["n"]], "z >= threshold, n >= min_count"))
    flagged_no_reason = fact.filter(F.col("is_flagged") & (F.size("reason_codes") == 0)).count()
    out.append(_c("gold.flagged_reviews_have_reasons", "error", flagged_no_reason == 0, flagged_no_reason, "0"))
    return out


def enforce(checks: list[Check]) -> dict:
    report = {"checks": [asdict(c) for c in checks],
              "n_checks": len(checks),
              "n_passed": sum(c.passed for c in checks),
              "n_failed_error": sum((not c.passed) and c.severity == "error" for c in checks),
              "n_failed_warn": sum((not c.passed) and c.severity == "warn" for c in checks)}
    report["status"] = "PASS" if report["n_failed_error"] == 0 else "FAIL"
    return report
