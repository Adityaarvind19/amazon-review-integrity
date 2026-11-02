# 06_reason_code_mix

```sql
-- Why reviews were flagged (explainability): reason-code frequency among flagged reviews
SELECT code, count(*) AS flagged_reviews, round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct
FROM (SELECT unnest(reason_codes) AS code FROM fact_review WHERE is_flagged)
GROUP BY code ORDER BY flagged_reviews DESC;
```

8 rows · 30.1 ms (DuckDB)

| code | flagged_reviews | pct |
|---|---|---|
| REVIEW_BURST | 42475 | 41.18 |
| UNVERIFIED | 19696 | 19.1 |
| COPY_PASTE | 18060 | 17.51 |
| SAME_DAY_SPREE | 14954 | 14.5 |
| RATING_TEXT_MISMATCH | 3966 | 3.85 |
| LOCKSTEP_RING | 3739 | 3.63 |
| INCENTIVIZED_DISCLOSURE | 215 | 0.21 |
| NEAR_DUP_CAMPAIGN | 31 | 0.03 |
