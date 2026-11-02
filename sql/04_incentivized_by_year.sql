SELECT d.year, count(*) AS reviews, round(100 * avg(f.is_incentivized::INT), 3) AS incentivized_pct,
       round(100 * avg(f.verified_purchase::INT), 2) AS verified_pct
FROM fact_review f JOIN dim_date d USING (date_key)
GROUP BY d.year ORDER BY d.year;
