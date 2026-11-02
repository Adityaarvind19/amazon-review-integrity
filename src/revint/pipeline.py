"""End-to-end orchestration + run manifest (lineage, row counts, timings, environment)."""
from __future__ import annotations

import json
import os
import platform
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import functions as F

from . import gold, quality, scoring, silver
from .config import lake
from .detectors import burst as burst_det
from .detectors import near_dup, text_signals
from .detectors import rings as ring_det
from .ingest import ingest, mark_complete
from .spark import get_spark, path_size_bytes, read_categories, read_categories_or_none, write_by_category


class Timer:
    def __init__(self):
        self.stages: dict[str, float] = {}

    @contextmanager
    def stage(self, name: str):
        t0 = time.perf_counter()
        print(f"[revint] {name} ...", flush=True)
        yield
        self.stages[name] = round(time.perf_counter() - t0, 2)
        print(f"[revint] {name} done in {self.stages[name]}s", flush=True)


def environment(spark) -> dict:
    mem = None
    try:
        mem = round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30, 1)
    except (ValueError, OSError, AttributeError):
        pass
    return {"platform": platform.platform(), "machine": platform.machine(), "cpu_count": os.cpu_count(),
            "memory_gb": mem, "python": sys.version.split()[0], "spark": spark.version,
            "java": spark.sparkContext._jvm.java.lang.System.getProperty("java.version"),
            "spark_master": spark.sparkContext.master,
            "spark_default_parallelism": spark.sparkContext.defaultParallelism}


def _features_or_empty(spark, path, cats, empty_df):
    df = read_categories_or_none(spark, path, cats)
    return empty_df if df is None else df


def run(cfg: dict, categories: list[str] | None = None, force: bool = False, publish: bool = False,
        tag: str = "main") -> dict:
    categories = categories or cfg["pipeline"]["categories"]
    timer = Timer()
    t_start = time.perf_counter()
    spark = get_spark(cfg)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    manifest: dict = {"run_id": run_id, "tag": tag, "categories_requested": categories,
                      "environment": environment(spark), "config": cfg}

    with timer.stage("ingest_bronze"):
        ing = ingest(spark, cfg, categories, force=force)
    todo = categories if force else ing["changed"]
    manifest.update(ingest=ing, categories_processed=todo)
    if not todo:
        print("[revint] no input changed: nothing to do (incremental no-op)")
        manifest.update(status="NOOP", stage_seconds=timer.stages)
        _write_manifest(cfg, manifest, publish, tag)
        return manifest

    with timer.stage("silver"):
        silver_stats = silver.build_reviews(spark, cfg, todo)
        product_stats = silver.build_products(spark, cfg, todo)
    with timer.stage("dq_gate_silver"):
        dq_silver = quality.silver_checks(spark, cfg, todo, ing["files"], silver_stats)
        rep = quality.enforce(dq_silver)
        if rep["status"] != "PASS":
            _fail(cfg, manifest, rep, publish, tag)

    reviews = silver.silver_reviews(spark, cfg, todo)
    products = silver.silver_products(spark, cfg, todo)
    feats = lake(cfg, "features")

    with timer.stage("text_features"):
        tf = text_signals.compute(reviews, cfg)
        write_by_category(spark, tf, f"{feats}/text_features", todo)
        text = read_categories(spark, f"{feats}/text_features", todo)

    with timer.stage("near_duplicates"):
        dup = near_dup.detect(reviews, text, cfg)
        write_by_category(spark, dup["edges"], f"{feats}/near_dup_edges", todo)
        write_by_category(spark, dup["members"], f"{feats}/near_dup_members", todo)
        write_by_category(spark, dup["clusters"], f"{feats}/near_dup_clusters", todo, num_files=1)
    with timer.stage("bursts"):
        bur = burst_det.detect(reviews, cfg)
        write_by_category(spark, bur["bursts"], f"{feats}/bursts", todo, num_files=1)
        write_by_category(spark, bur["members"], f"{feats}/burst_members", todo)
    with timer.stage("review_rings"):
        rg = ring_det.detect(reviews, cfg)
        write_by_category(spark, rg["rings"], f"{feats}/rings", todo, num_files=1)
        write_by_category(spark, rg["members"], f"{feats}/ring_members", todo)

    # re-read persisted features so downstream joins never recompute detectors
    dup = {"members": _features_or_empty(spark, f"{feats}/near_dup_members", todo, dup["members"]),
           "clusters": _features_or_empty(spark, f"{feats}/near_dup_clusters", todo, dup["clusters"]),
           "stats": dup["stats"]}
    bur = {"members": _features_or_empty(spark, f"{feats}/burst_members", todo, bur["members"]),
           "stats": bur["stats"]}
    rg = {"members": _features_or_empty(spark, f"{feats}/ring_members", todo, rg["members"]),
          "stats": rg["stats"]}

    with timer.stage("scoring"):
        scored = scoring.score_reviews(reviews, text, dup, rg, bur, cfg).persist(StorageLevel.DISK_ONLY)
        product_mart = scoring.product_integrity(scored, cfg).persist(StorageLevel.DISK_ONLY)
    with timer.stage("gold_star_schema_and_marts"):
        gold.build_fact_and_dim_product(spark, cfg, todo, scored, products)
        gold.build_marts(spark, cfg, todo, scored, product_mart, products, dup, rg, bur)
        gold.build_global_dims(spark, cfg)
    with timer.stage("dq_gate_gold"):
        dq_gold = quality.gold_checks(spark, cfg, todo)
        rep = quality.enforce(dq_silver + dq_gold)
        if rep["status"] != "PASS":
            _fail(cfg, manifest, rep, publish, tag)

    # summary covers every requested category (incremental runs only rebuild `todo`)
    mark_complete(cfg["paths"]["state_dir"], todo)
    fact = read_categories(spark, lake(cfg, "gold", "fact_review"), categories)
    summary = fact.agg(F.count("*").alias("reviews"), F.sum(F.col("is_flagged").cast("int")).alias("flagged"),
                       F.countDistinct("product_key").alias("products"),
                       F.countDistinct("reviewer_key").alias("reviewers")).first().asDict()
    pm = read_categories(spark, lake(cfg, "gold", "mart_product_integrity"), categories)
    summary["products_grade_D_or_F"] = pm.filter(F.col("integrity_grade").isin("D", "F")).count()
    summary["products_inflated_ge_0_25_stars"] = pm.filter(F.col("rating_inflation") >= 0.25).count()
    reason_counts = {r["code"]: r["n"] for r in fact.select(F.explode("reason_codes").alias("code"))
                     .groupBy("code").agg(F.count("*").alias("n")).collect()}
    manifest.update(
        status="SUCCESS",
        silver=silver_stats, products=product_stats,
        detectors={"near_dup": dup["stats"], "burst": bur["stats"], "rings": rg["stats"]},
        summary=summary, reason_code_counts=reason_counts, dq=rep,
        storage_bytes={
            "raw_input": sum(f["size"] for c in categories for f in ing["files"][c]["files"].values()),
            "bronze": path_size_bytes(spark, lake(cfg, "bronze")),
            "silver": path_size_bytes(spark, lake(cfg, "silver")),
            "features": path_size_bytes(spark, lake(cfg, "features")),
            "gold": path_size_bytes(spark, lake(cfg, "gold"))},
        stage_seconds=timer.stages,
        total_seconds=round(time.perf_counter() - t_start, 2))
    scored.unpersist()
    product_mart.unpersist()
    _write_manifest(cfg, manifest, publish, tag)
    print(f"[revint] SUCCESS {summary} in {manifest['total_seconds']}s; DQ {rep['n_passed']}/{rep['n_checks']} passed")
    return manifest


def _fail(cfg, manifest, rep, publish, tag):
    manifest.update(status="DQ_FAILED", dq=rep)
    _write_manifest(cfg, manifest, publish, tag)
    failed = [c["name"] for c in rep["checks"] if not c["passed"] and c["severity"] == "error"]
    raise quality.DataQualityError(f"data-quality gate failed: {failed}")


def _write_manifest(cfg, manifest, publish, tag):
    runs = Path(cfg["paths"]["state_dir"], "runs")
    runs.mkdir(parents=True, exist_ok=True)
    text = json.dumps(manifest, indent=2, default=str)
    (runs / f"{manifest['run_id']}_{tag}.json").write_text(text)
    if publish:
        res = Path(cfg["paths"]["results_dir"])
        res.mkdir(parents=True, exist_ok=True)
        (res / f"run_manifest_{tag}.json").write_text(text)
        if "dq" in manifest:
            (res / f"dq_report_{tag}.json").write_text(json.dumps(manifest["dq"], indent=2, default=str))
