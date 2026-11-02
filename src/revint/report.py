"""Generate docs/results/RESUME_NUMBERS.md: every resume-worthy number with its evidence file + JSON path.

Nothing here is typed by hand; if an evidence file is missing, the number is reported as MISSING.
"""
from __future__ import annotations

import json
from pathlib import Path


def _load(p: Path):
    return json.loads(p.read_text()) if p.exists() else None


def _get(d, path):
    for k in path.split("."):
        if d is None:
            return None
        d = d.get(k) if isinstance(d, dict) else None
    return d


def _fmt_n(x):
    if x is None:
        return "MISSING"
    if isinstance(x, float):
        return f"{x:,.2f}"
    return f"{x:,}"


def _pct(x):
    return "MISSING" if x is None else f"{x * 100:.1f}%"


def _human(x):
    if x is None:
        return "MISSING"
    for unit, div in (("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if x >= div:
            return f"{x / div:.1f}{unit}"
    return str(x)


def write_resume_numbers(cfg: dict) -> str:
    res = Path(cfg["paths"]["results_dir"])
    man = _load(res / "run_manifest_main.json")
    ev = _load(res / "eval" / "eval_report.json")
    idem = _load(res / "idempotency_check.json")
    rows = []

    def add(label, value, src, key):
        rows.append((label, value, src, key))

    m = "run_manifest_main.json"
    s = _get(man, "summary") or {}
    add("Reviews processed (silver/fact)", _fmt_n(s.get("reviews")), m, "summary.reviews")
    add("Products / reviewers", f"{_fmt_n(s.get('products'))} / {_fmt_n(s.get('reviewers'))}", m, "summary.products/reviewers")
    add("End-to-end runtime (s)", _fmt_n(_get(man, "total_seconds")), m, "total_seconds")
    add("Raw input bytes", _fmt_n(_get(man, "storage_bytes.raw_input")), m, "storage_bytes.raw_input")
    add("Flagged reviews", _fmt_n(s.get("flagged")), m, "summary.flagged")
    add("Products graded D/F", _fmt_n(s.get("products_grade_D_or_F")), m, "summary.products_grade_D_or_F")
    add("Products inflated >= 0.25 stars", _fmt_n(s.get("products_inflated_ge_0_25_stars")), m, "summary.products_inflated_ge_0_25_stars")
    nd = _get(man, "detectors.near_dup") or {}
    add("LSH-eligible reviews", _fmt_n(nd.get("lsh_eligible_reviews")), m, "detectors.near_dup.lsh_eligible_reviews")
    add("Naive pairwise comparisons", _human(nd.get("naive_pairs")), m, "detectors.near_dup.naive_pairs")
    add("LSH candidate edges", _human(nd.get("lsh_candidate_edges")), m, "detectors.near_dup.lsh_candidate_edges")
    add("Pair reduction factor", _fmt_n(nd.get("pair_reduction_factor")), m, "detectors.near_dup.pair_reduction_factor")
    add("Connected-components iterations", _fmt_n(nd.get("cc_iterations")), m, "detectors.near_dup.cc_iterations")
    add("Review rings found", _fmt_n(_get(man, "detectors.rings.rings")), m, "detectors.rings.rings")
    add("Burst weeks found", _fmt_n(_get(man, "detectors.burst.burst_weeks")), m, "detectors.burst.burst_weeks")
    add("DQ checks passed", f"{_get(man, 'dq.n_passed')}/{_get(man, 'dq.n_checks')}", m, "dq.n_passed / dq.n_checks")
    add("Quarantined rows", _fmt_n(sum((v or {}).get("quarantined", 0) for v in (_get(man, "silver") or {}).values())
                                   if man else None), m, "silver.<cat>.quarantined")
    e = "eval/eval_report.json"
    a = _get(ev, "aggregate") or {}
    add("Eval seeds", str(_get(ev, "seeds")), e, "seeds")
    add("Review recall (combined)", _pct(_get(a, "recall_combined.mean")), e, "aggregate.recall_combined.mean")
    add("Precision on targeted products", _pct(_get(a, "precision_on_target_products.mean")), e,
        "aggregate.precision_on_target_products.mean")
    add("Precision lower bound", _pct(_get(a, "precision_lower_bound.mean")), e, "aggregate.precision_lower_bound.mean")
    add("Organic flag rate", _pct(_get(a, "organic_flag_rate.mean")), e, "aggregate.organic_flag_rate.mean")
    add("Product AUC", _fmt_n(_get(a, "product_auc.mean")), e, "aggregate.product_auc.mean")
    add("Product precision@K", _pct(_get(a, "product_precision_at_k.mean")), e, "aggregate.product_precision_at_k.mean")
    add("Rating distortion removed", _pct(_get(a, "rating_distortion_removed.mean")), e, "aggregate.rating_distortion_removed.mean")
    for t, v in (a.get("recall_by_campaign_type") or {}).items():
        add(f"Recall: {t}", _pct(_get(v, "mean")), e, f"aggregate.recall_by_campaign_type.{t}.mean")
    i = "idempotency_check.json"
    add("Forced rerun identical (all tables)", str(_get(idem, "idempotent")), i, "idempotent")
    add("Incremental rerun status", str(_get(idem, "incremental_rerun_status")), i, "incremental_rerun_status")

    lines = ["# Resume numbers (generated — do not edit by hand)", "",
             "Every value below is read from an evidence file in this folder. Regenerate with "
             "`python -m revint report`. If a value says MISSING, do not put it on the resume.", "",
             "| Metric | Value | Evidence file | JSON key |", "|---|---|---|---|"]
    lines += [f"| {a_} | {b_} | `{c_}` | `{d_}` |" for a_, b_, c_, d_ in rows]
    env = _get(man, "environment") or {}
    lines += ["", f"Machine: {env.get('platform')} · {env.get('cpu_count')} CPUs · {env.get('memory_gb')} GB · "
              f"Spark {env.get('spark')} ({env.get('spark_master')}) · Java {env.get('java')} · Python {env.get('python')}"]
    out = res / "RESUME_NUMBERS.md"
    out.write_text("\n".join(lines) + "\n")
    return str(out)
