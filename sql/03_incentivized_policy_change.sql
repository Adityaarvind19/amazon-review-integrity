-- Natural experiment: Amazon banned incentivized reviews on 2016-10-03. Disclosure rate and rating gap before/after.
SELECT f.category, d.is_post_incentive_ban AS post_ban, count(*) AS reviews,
       round(100 * avg(f.is_incentivized::INT), 3) AS incentivized_pct,
       round(avg(CASE WHEN f.is_incentivized THEN f.rating END), 3) AS avg_rating_incentivized,
       round(avg(CASE WHEN NOT f.is_incentivized THEN f.rating END), 3) AS avg_rating_other
FROM fact_review f JOIN dim_date d USING (date_key)
GROUP BY ALL ORDER BY f.category, post_ban;
