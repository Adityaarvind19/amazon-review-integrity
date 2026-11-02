-- Largest detected campaigns across detectors
SELECT category, campaign_type, subtype, campaign_id, n_reviews, n_reviewers, n_products,
       round(avg_rating, 2) AS avg_rating, round(verified_share, 2) AS verified_share,
       first_ts::DATE AS first_day, last_ts::DATE AS last_day, example_parent_asin
FROM mart_campaigns
ORDER BY n_reviews DESC LIMIT 25;
