# Detector evaluation — All_Beauty

Seeds: [11, 22, 33] · campaigns per type per seed: 20 · mean ± std across seeds. Synthetic campaigns injected into the real raw file; full pipeline rerun per seed.

| Metric | Value |
|---|---|
| Review recall (combined flag) | 85.6% ± 0.5 |
| Precision, lower bound (every flagged organic review counted as FP) | 57.8% ± 1.2 |
| Precision on targeted products | 99.2% ± 0.2 |
| Organic review flag rate | 0.3% ± 0.0 |
| Product AUC (100 − integrity score) | 0.982 ± 0.002 |
| Product precision@K / recall@K (K = #targets) | 77.1% ± 1.6 / 77.1% ± 1.6 |
| Rating distortion removed by trust-adjusted rating | 86.3% ± 0.4 |

## Recall by campaign type

| Type | Recall |
|---|---|
| TEMPLATE | 99.7% ± 0.4 |
| REVIEW_BOMB | 99.3% ± 0.1 |
| RING | 99.3% ± 0.5 |
| BURST | 97.2% ± 0.7 |
| SLOW_DRIP | 0.0% ± 0.0 |

## Recall by single detector (target reviews)

| Detector | Recall |
|---|---|
| near_dup | 30.2% ± 2.2 |
| copy_paste | 0.0% ± 0.0 |
| ring | 27.0% ± 2.3 |
| burst | 63.4% ± 0.4 |
| mismatch | 0.0% ± 0.0 |

Caveat: organic reviews are unlabeled, so real fakes in the data count as false positives here; precision is a lower bound. SLOW_DRIP is a deliberately hard case.
