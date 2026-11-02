"""SQL analytics over the gold star schema with DuckDB (the same SQL ports to Athena/Redshift)."""
from __future__ import annotations

import json
import time
from pathlib import Path

import duckdb

from .config import REPO_ROOT, lake

TABLES = ["fact_review", "dim_product", "dim_reviewer", "dim_date", "dim_category", "mart_product_integrity",
          "mart_store_integrity", "mart_category_monthly", "mart_campaigns"]


def connect(cfg: dict) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    for t in TABLES:
        root = Path(lake(cfg, "gold", t))
        if not any(root.rglob("*.parquet")):
            continue
        con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{root}/**/*.parquet', hive_partitioning=true)")
    return con


def to_md(df) -> str:
    cols = list(df.columns)
    rows = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for r in df.itertuples(index=False):
        rows.append("| " + " | ".join("" if v is None else str(v) for v in r) + " |")
    return "\n".join(rows)


def run_sql(cfg: dict, out: str | None = None) -> dict:
    con = connect(cfg)
    out_dir = Path(out or Path(cfg["paths"]["results_dir"], "sql"))
    out_dir.mkdir(parents=True, exist_ok=True)
    timings = {}
    for f in sorted((REPO_ROOT / "sql").glob("*.sql")):
        q = f.read_text()
        t0 = time.perf_counter()
        try:
            df = con.execute(q).df()
        except duckdb.Error as e:
            (out_dir / f"{f.stem}.md").write_text(f"# {f.stem}\n\nQuery failed: {e}\n")
            timings[f.stem] = None
            continue
        ms = round((time.perf_counter() - t0) * 1000, 1)
        timings[f.stem] = ms
        (out_dir / f"{f.stem}.md").write_text(
            f"# {f.stem}\n\n```sql\n{q.strip()}\n```\n\n{len(df)} rows · {ms} ms (DuckDB)\n\n{to_md(df)}\n")
        print(f"[sql] {f.stem}: {len(df)} rows in {ms} ms")
    (out_dir / "timings_ms.json").write_text(json.dumps(timings, indent=2))
    return timings
