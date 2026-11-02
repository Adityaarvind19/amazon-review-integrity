"""CLI: python -m revint <command>"""
from __future__ import annotations

import argparse
import json

from .config import load_config


def main(argv=None):
    ap = argparse.ArgumentParser(prog="revint")
    ap.add_argument("--config", default=None)
    ap.add_argument("--raw-dir")
    ap.add_argument("--lake-dir")
    ap.add_argument("--state-dir")
    ap.add_argument("--results-dir")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="ingest -> silver -> detectors -> gold -> DQ gate")
    r.add_argument("--categories", nargs="+")
    r.add_argument("--force", action="store_true", help="reprocess even if inputs are unchanged")
    r.add_argument("--publish", action="store_true", help="write manifest + DQ report to results dir")
    r.add_argument("--tag", default="main", help="manifest tag, e.g. main / incremental")
    e = sub.add_parser("eval", help="inject seeded campaigns, rerun pipeline, measure precision/recall")
    e.add_argument("--category", required=True)
    e.add_argument("--seeds", nargs="+", type=int)
    e.add_argument("--campaigns-per-type", type=int)
    e.add_argument("--work-dir", default="data/eval")
    s = sub.add_parser("sql", help="run sql/*.sql on gold tables with DuckDB")
    s.add_argument("--out", default=None)
    sub.add_parser("dashboard", help="build docs/results/dashboard.html")
    sub.add_parser("report", help="write docs/results/RESUME_NUMBERS.md from committed evidence")
    b = sub.add_parser("idempotency", help="rerun with --force and compare gold checksums")
    b.add_argument("--categories", nargs="+")
    a = ap.parse_args(argv)
    cfg = load_config(a.config, raw_dir=a.raw_dir, lake_dir=a.lake_dir, state_dir=a.state_dir,
                      results_dir=a.results_dir)

    if a.cmd == "run":
        from .pipeline import run
        run(cfg, a.categories, force=a.force, publish=a.publish, tag=a.tag)
    elif a.cmd == "eval":
        from .evaluate import run_eval
        rep = run_eval(cfg, a.category, a.seeds or cfg["eval"]["seeds"],
                       a.campaigns_per_type or cfg["eval"]["campaigns_per_type"], a.work_dir)
        print(json.dumps(rep["aggregate"], indent=2))
    elif a.cmd == "sql":
        from .analytics import run_sql
        run_sql(cfg, a.out)
    elif a.cmd == "dashboard":
        from .dashboard import build
        print(build(cfg))
    elif a.cmd == "report":
        from .report import write_resume_numbers
        print(write_resume_numbers(cfg))
    elif a.cmd == "idempotency":
        from .idempotency import check
        print(json.dumps(check(cfg, a.categories), indent=2))


if __name__ == "__main__":
    main()
