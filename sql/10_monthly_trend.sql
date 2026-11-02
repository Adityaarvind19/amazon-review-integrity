-- Monthly flagged share and raw vs trusted average rating (last 36 months with data)
SELECT category, year_month, n_reviews, round(100 * flagged_share, 2) AS flagged_pct,
       round(raw_avg_rating, 3) AS raw_avg, round(trusted_avg_rating, 3) AS trusted_avg,
       round(raw_avg_rating - trusted_avg_rating, 3) AS gap
FROM mart_category_monthly
QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY year_month DESC) <= 36
ORDER BY category, year_month;
