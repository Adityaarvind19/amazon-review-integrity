"""Idempotency + incrementality proof: checksum gold/feature tables, rerun, compare."""
from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, FloatType

from . import pipeline
from .config import lake
from .spark import get_spark, read_categories_or_none

TABLES = ["gold/fact_review", "gold/dim_product", "gold/mart_product_integrity", "gold/mart_store_integrity",
          "gold/mart_category_monthly", "gold/mart_campaigns", "features/near_dup_edges", "features/bursts",
          "features/rings"]


def table_checksum(df: DataFrame) -> dict:
    """Order-independent content checksum: sum of per-row 64-bit hashes (floats rounded to 9 dp)."""
    cols = [F.round(F.col(f.name), 9) if isinstance(f.dataType, (DoubleType, FloatType)) else F.col(f.name)
            for f in sorted(df.schema.fields, key=lambda f: f.name)]
    r = df.select(F.xxhash64(*cols).cast("decimal(38,0)").alias("h")).agg(
        F.count("*").alias("rows"), F.sum("h").alias("hash_sum")).first()
    return {"rows": r["rows"], "hash_sum": str(r["hash_sum"])}


def checksums(spark, cfg, categories) -> dict:
    out = {}
    for t in TABLES:
        df = read_categories_or_none(spark, lake(cfg, *t.split("/")), categories)
        out[t] = table_checksum(df) if df is not None else {"rows": 0, "hash_sum": "0"}
    return out


def check(cfg: dict, categories: list[str] | None = None, publish: bool = True) -> dict:
    categories = categories or cfg["pipeline"]["categories"]
    spark = get_spark(cfg)
    before = checksums(spark, cfg, categories)
    noop = pipeline.run(cfg, categories, force=False, tag="incremental_noop")
    forced = pipeline.run(cfg, categories, force=True, tag="idempotency_rerun")
    after = checksums(spark, cfg, categories)
    result = {"categories": categories,
              "incremental_rerun_status": noop["status"],
              "incremental_rerun_seconds": noop["stage_seconds"],
              "forced_rerun_seconds": forced.get("total_seconds"),
              "tables_identical": {t: before[t] == after[t] for t in TABLES},
              "before": before, "after": after}
    result["idempotent"] = all(result["tables_identical"].values())
    if publish:
        p = Path(cfg["paths"]["results_dir"])
        p.mkdir(parents=True, exist_ok=True)
        (p / "idempotency_check.json").write_text(json.dumps(result, indent=2))
    return result
