# Resume numbers (generated — do not edit by hand)

Every value below is read from an evidence file in this folder. Regenerate with `python -m revint report`. If a value says MISSING, do not put it on the resume.

| Metric | Value | Evidence file | JSON key |
|---|---|---|---|
| Reviews processed (silver/fact) | 5,265,219 | `run_manifest_main.json` | `summary.reviews` |
| Products / reviewers | 249,814 / 3,332,339 | `run_manifest_main.json` | `summary.products/reviewers` |
| End-to-end runtime (s) | 577.61 | `run_manifest_main.json` | `total_seconds` |
| Raw input bytes | 3,659,925,951 | `run_manifest_main.json` | `storage_bytes.raw_input` |
| Flagged reviews | 65,507 | `run_manifest_main.json` | `summary.flagged` |
| Products graded D/F | 627 | `run_manifest_main.json` | `summary.products_grade_D_or_F` |
| Products inflated >= 0.25 stars | 23 | `run_manifest_main.json` | `summary.products_inflated_ge_0_25_stars` |
| LSH-eligible reviews | 3,854,419 | `run_manifest_main.json` | `detectors.near_dup.lsh_eligible_reviews` |
| Naive pairwise comparisons | 7428.3B | `run_manifest_main.json` | `detectors.near_dup.naive_pairs` |
| LSH candidate edges | 169.6K | `run_manifest_main.json` | `detectors.near_dup.lsh_candidate_edges` |
| Pair reduction factor | 43,801,091.96 | `run_manifest_main.json` | `detectors.near_dup.pair_reduction_factor` |
| Connected-components iterations | 7 | `run_manifest_main.json` | `detectors.near_dup.cc_iterations` |
| Review rings found | 377 | `run_manifest_main.json` | `detectors.rings.rings` |
| Burst weeks found | 3,594 | `run_manifest_main.json` | `detectors.burst.burst_weeks` |
| DQ checks passed | 26/26 | `run_manifest_main.json` | `dq.n_passed / dq.n_checks` |
| Quarantined rows | 0 | `run_manifest_main.json` | `silver.<cat>.quarantined` |
| Eval seeds | [11, 22, 33] | `eval/eval_report.json` | `seeds` |
| Review recall (combined) | 85.6% | `eval/eval_report.json` | `aggregate.recall_combined.mean` |
| Precision on targeted products | 99.2% | `eval/eval_report.json` | `aggregate.precision_on_target_products.mean` |
| Precision lower bound | 57.8% | `eval/eval_report.json` | `aggregate.precision_lower_bound.mean` |
| Organic flag rate | 0.3% | `eval/eval_report.json` | `aggregate.organic_flag_rate.mean` |
| Product AUC | 0.98 | `eval/eval_report.json` | `aggregate.product_auc.mean` |
| Product precision@K | 77.1% | `eval/eval_report.json` | `aggregate.product_precision_at_k.mean` |
| Rating distortion removed | 86.3% | `eval/eval_report.json` | `aggregate.rating_distortion_removed.mean` |
| Recall: TEMPLATE | 99.7% | `eval/eval_report.json` | `aggregate.recall_by_campaign_type.TEMPLATE.mean` |
| Recall: REVIEW_BOMB | 99.3% | `eval/eval_report.json` | `aggregate.recall_by_campaign_type.REVIEW_BOMB.mean` |
| Recall: RING | 99.3% | `eval/eval_report.json` | `aggregate.recall_by_campaign_type.RING.mean` |
| Recall: BURST | 97.2% | `eval/eval_report.json` | `aggregate.recall_by_campaign_type.BURST.mean` |
| Recall: SLOW_DRIP | 0.0% | `eval/eval_report.json` | `aggregate.recall_by_campaign_type.SLOW_DRIP.mean` |
| Forced rerun identical (all tables) | True | `idempotency_check.json` | `idempotent` |
| Incremental rerun status | NOOP | `idempotency_check.json` | `incremental_rerun_status` |

Machine: macOS-26.6.2-arm64-arm-64bit-Mach-O · 10 CPUs · 16.0 GB · Spark 4.3.0 (local[*]) · Java 17.0.21.0.3 · Python 3.14.6
