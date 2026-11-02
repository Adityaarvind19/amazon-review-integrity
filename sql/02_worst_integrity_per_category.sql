-- Top-N per group: 5 lowest-integrity products per category (window function)
SELECT * FROM (
  SELECT category, parent_asin, left(product_title, 50) AS title, n_reviews, integrity_score, flagged_share,
         ROW_NUMBER() OVER (PARTITION BY category ORDER BY integrity_score, n_reviews DESC) AS rk
  FROM mart_product_integrity WHERE n_reviews >= 20)
WHERE rk <= 5 ORDER BY category, rk;
