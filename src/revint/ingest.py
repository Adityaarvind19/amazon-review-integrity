"""Bronze layer: raw JSONL(.gz) -> Parquet, incremental at file granularity.

A JSON manifest records (size, sha256) for every ingested file. A rerun skips unchanged
files, so the downstream layers are rebuilt only for categories whose input changed.
"""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import lake
from .schemas import META_SCHEMA, REVIEW_SCHEMA
from .spark import write_by_category

MANIFEST = "ingest_manifest.json"


def sha256_file(path: Path, chunk: int = 1 << 22) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while b := f.read(chunk):
            h.update(b)
    return h.hexdigest()


def find_file(raw_dir: Path, stem: str) -> Path | None:
    for suffix in (".jsonl.gz", ".jsonl", ".json.gz", ".json"):
        p = raw_dir / f"{stem}{suffix}"
        if p.exists():
            return p
    return None


def load_manifest(state_dir: str) -> dict:
    p = Path(state_dir, MANIFEST)
    return json.loads(p.read_text()) if p.exists() else {}


def save_manifest(state_dir: str, manifest: dict) -> None:
    Path(state_dir).mkdir(parents=True, exist_ok=True)
    Path(state_dir, MANIFEST).write_text(json.dumps(manifest, indent=2, sort_keys=True))


def _ingest_one(spark: SparkSession, cfg: dict, path: Path, schema, table: str, category: str) -> dict:
    df = (spark.read.schema(schema)
          .option("mode", "PERMISSIVE")
          .option("columnNameOfCorruptRecord", "_corrupt_record")
          .json(str(path))
          .withColumn("category", F.lit(category))
          .withColumn("_source_file", F.lit(path.name)))
    # gzip is not splittable: one task reads it, then we fan out for the Parquet write
    approx_raw = path.stat().st_size * (5 if path.suffix == ".gz" else 1)
    files = max(1, math.ceil(approx_raw / (256 << 20)))
    write_by_category(spark, df, lake(cfg, "bronze", table), [category], num_files=files)
    bronze_rows = spark.read.parquet(lake(cfg, "bronze", table, f"category={category}")).count()
    source_lines = (spark.read.text(str(path)).filter(F.length(F.trim("value")) > 0).count())
    return {"bronze_rows": bronze_rows, "source_lines": source_lines}


def ingest(spark: SparkSession, cfg: dict, categories: list[str], force: bool = False) -> dict:
    """Returns {"changed": [...], "skipped": [...], "files": {...}}."""
    raw_dir = Path(cfg["paths"]["raw_dir"])
    manifest = load_manifest(cfg["paths"]["state_dir"])
    changed, skipped, files = [], [], {}
    for cat in categories:
        rev = find_file(raw_dir, cat)
        meta = find_file(raw_dir, f"meta_{cat}")
        if rev is None or meta is None:
            raise FileNotFoundError(f"missing raw files for {cat} in {raw_dir} (need {cat}.jsonl[.gz] and meta_{cat}.jsonl[.gz])")
        fps = {p.name: {"size": p.stat().st_size, "sha256": sha256_file(p)} for p in (rev, meta)}
        prev = manifest.get(cat, {}).get("files", {})
        unchanged = all(prev.get(n, {}).get("sha256") == fp["sha256"] for n, fp in fps.items())
        if unchanged and not force and manifest.get(cat, {}).get("status") == "complete":
            skipped.append(cat)
            files[cat] = manifest[cat]
            continue
        r = _ingest_one(spark, cfg, rev, REVIEW_SCHEMA, "reviews", cat)
        m = _ingest_one(spark, cfg, meta, META_SCHEMA, "products", cat)
        for n, fp in fps.items():
            fp.update(r if n == rev.name else m)
        manifest[cat] = {"status": "ingested", "files": fps}
        save_manifest(cfg["paths"]["state_dir"], manifest)
        changed.append(cat)
        files[cat] = manifest[cat]
    return {"changed": changed, "skipped": skipped, "files": files}


def mark_complete(state_dir: str, categories: list[str]) -> None:
    """Called by the pipeline only after the gold DQ gate passes."""
    manifest = load_manifest(state_dir)
    for c in categories:
        if c in manifest:
            manifest[c]["status"] = "complete"
    save_manifest(state_dir, manifest)
