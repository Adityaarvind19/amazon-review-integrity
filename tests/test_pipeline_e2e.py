import json
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from revint import pipeline, sample
from revint.config import lake
from revint.idempotency import checksums
from revint.quality import DataQualityError
from revint.spark import read_categories


@pytest.fixture(scope="module")
def cfg(make_cfg):
    c = make_cfg("e2e")
    sample.generate(c["paths"]["raw_dir"], "All_Beauty", n_products=250, n_users=1500, n_reviews=5000, seed=7)
    return c


def test_full_run_passes_dq_and_builds_star_schema(cfg, spark):
    man = pipeline.run(cfg, ["All_Beauty"], publish=True)
    assert man["status"] == "SUCCESS" and man["dq"]["status"] == "PASS"
    s = man["silver"]["All_Beauty"]
    assert s["silver_rows"] + s["quarantined"] + s["duplicates_removed"] == s["bronze_rows"]
    assert set(s["quarantine_reasons"]) == {"MALFORMED_JSON", "BAD_RATING", "MISSING_USER_ID"}
    assert s["duplicates_removed"] > 0
    for t in ("fact_review", "dim_product", "mart_product_integrity", "mart_category_monthly"):
        assert read_categories(spark, lake(cfg, "gold", t), ["All_Beauty"]).count() > 0
    assert spark.read.parquet(lake(cfg, "gold", "dim_date")).count() > 365
    inferred = read_categories(spark, lake(cfg, "gold", "dim_product"), ["All_Beauty"]).filter("is_inferred_member")
    assert inferred.count() > 0  # reviews of products missing from metadata keep FK integrity
    assert Path(cfg["paths"]["results_dir"], "run_manifest_main.json").exists()


def test_incremental_noop_and_idempotent_rerun(cfg, spark):
    before = checksums(spark, cfg, ["All_Beauty"])
    assert pipeline.run(cfg, ["All_Beauty"])["status"] == "NOOP"
    pipeline.run(cfg, ["All_Beauty"], force=True)
    assert checksums(spark, cfg, ["All_Beauty"]) == before


def test_new_category_is_processed_without_touching_existing(cfg, spark):
    before = checksums(spark, cfg, ["All_Beauty"])
    sample.generate(cfg["paths"]["raw_dir"], "Video_Games", n_products=150, n_users=900, n_reviews=3000, seed=8)
    man = pipeline.run(cfg, ["All_Beauty", "Video_Games"])
    assert man["categories_processed"] == ["Video_Games"]
    assert man["ingest"]["skipped"] == ["All_Beauty"]
    assert checksums(spark, cfg, ["All_Beauty"]) == before
    cats = {r[0] for r in spark.read.parquet(lake(cfg, "gold", "dim_category")).select("category").collect()}
    assert cats == {"All_Beauty", "Video_Games"}


def test_changed_file_triggers_reprocess(cfg, spark):
    sample.generate(cfg["paths"]["raw_dir"], "Video_Games", n_products=150, n_users=900, n_reviews=3200, seed=9)
    man = pipeline.run(cfg, ["All_Beauty", "Video_Games"])
    assert man["categories_processed"] == ["Video_Games"]
    n = read_categories(spark, lake(cfg, "gold", "fact_review"), ["Video_Games"]).count()
    assert n == man["silver"]["Video_Games"]["silver_rows"]


def test_dq_gate_blocks_bad_reconciliation(cfg, spark, monkeypatch):
    from revint import silver
    real = silver.build_reviews

    def broken(*a, **k):
        out = real(*a, **k)
        for v in out.values():
            v["duplicates_removed"] += 1  # simulate a silently dropped row
        return out
    monkeypatch.setattr(silver, "build_reviews", broken)
    with pytest.raises(DataQualityError):
        pipeline.run(cfg, ["All_Beauty"], force=True, tag="dq_negative_test")
    runs = sorted(Path(cfg["paths"]["state_dir"], "runs").glob("*dq_negative_test.json"))
    assert json.loads(runs[-1].read_text())["status"] == "DQ_FAILED"


def test_flagged_reviews_are_explained(cfg, spark):
    fact = read_categories(spark, lake(cfg, "gold", "fact_review"), ["All_Beauty"])
    assert fact.filter(F.col("is_flagged") & (F.size("reason_codes") == 0)).count() == 0
