-- Why reviews were flagged (explainability): reason-code frequency among flagged reviews
SELECT code, count(*) AS flagged_reviews, round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct
FROM (SELECT unnest(reason_codes) AS code FROM fact_review WHERE is_flagged)
GROUP BY code ORDER BY flagged_reviews DESC;
