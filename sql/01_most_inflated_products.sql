-- Products whose star rating is most inflated by suspicious reviews (trust-adjusted vs raw, same Bayesian prior)
SELECT p.category, p.parent_asin, left(p.product_title, 60) AS title, p.store, p.n_reviews, p.n_flagged,
       round(p.raw_bayes_rating, 2) AS raw_rating, round(p.trust_adjusted_rating, 2) AS trusted_rating,
       round(p.rating_inflation, 2) AS inflation, p.integrity_score, p.integrity_grade
FROM mart_product_integrity p
WHERE p.n_reviews >= 20
ORDER BY p.rating_inflation DESC, p.n_reviews DESC
LIMIT 20;
