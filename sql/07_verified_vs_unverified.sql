-- Rating gap between verified and unverified purchases, by year
SELECT d.year,
       round(avg(CASE WHEN f.verified_purchase THEN f.rating END), 3) AS verified_avg,
       round(avg(CASE WHEN NOT f.verified_purchase THEN f.rating END), 3) AS unverified_avg,
       round(100 * avg(f.is_flagged::INT), 2) AS flagged_pct
FROM fact_review f JOIN dim_date d USING (date_key)
GROUP BY d.year ORDER BY d.year;
