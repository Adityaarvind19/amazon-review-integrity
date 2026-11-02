# 03_incentivized_policy_change

```sql
-- Natural experiment: Amazon banned incentivized reviews on 2016-10-03. Disclosure rate and rating gap before/after.
SELECT f.category, d.is_post_incentive_ban AS post_ban, count(*) AS reviews,
       round(100 * avg(f.is_incentivized::INT), 3) AS incentivized_pct,
       round(avg(CASE WHEN f.is_incentivized THEN f.rating END), 3) AS avg_rating_incentivized,
       round(avg(CASE WHEN NOT f.is_incentivized THEN f.rating END), 3) AS avg_rating_other
FROM fact_review f JOIN dim_date d USING (date_key)
GROUP BY ALL ORDER BY f.category, post_ban;
```

4 rows · 36.7 ms (DuckDB)

| category | post_ban | reviews | incentivized_pct | avg_rating_incentivized | avg_rating_other |
|---|---|---|---|---|---|
| All_Beauty | False | 115801 | 1.694 | 4.571 | 4.135 |
| All_Beauty | True | 578451 | 0.015 | 3.92 | 3.924 |
| Video_Games | False | 1732881 | 0.087 | 4.207 | 4.103 |
| Video_Games | True | 2838086 | 0.021 | 3.696 | 4.015 |
