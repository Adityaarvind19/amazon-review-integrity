-- Store/brand roll-up: stores with the most low-integrity products (min 5 products)
SELECT category, store, n_products, n_reviews, round(100 * flagged_share, 2) AS flagged_pct,
       round(weighted_integrity, 1) AS weighted_integrity, n_low_integrity_products,
       round(avg_rating_inflation, 3) AS avg_inflation
FROM mart_store_integrity WHERE n_products >= 5
ORDER BY weighted_integrity ASC, n_reviews DESC LIMIT 20;
