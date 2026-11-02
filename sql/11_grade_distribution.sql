SELECT category, integrity_grade, count(*) AS products, sum(n_reviews) AS reviews,
       round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY category), 2) AS pct_products
FROM mart_product_integrity WHERE n_reviews >= 10
GROUP BY ALL ORDER BY category, integrity_grade;
