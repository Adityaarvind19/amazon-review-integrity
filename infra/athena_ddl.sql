-- UNTESTED SKETCH (not executed): Athena / Glue catalog table over the gold fact table.
CREATE EXTERNAL TABLE IF NOT EXISTS review_integrity.fact_review (
  review_id BIGINT, date_key INT, product_key BIGINT, reviewer_key BIGINT, review_ts TIMESTAMP, rating DOUBLE,
  verified_purchase BOOLEAN, helpful_vote BIGINT, sentiment DOUBLE, is_incentivized BOOLEAN, is_mismatch BOOLEAN,
  dup_cluster_id BIGINT, ring_id BIGINT, burst_z DOUBLE, suspicion DOUBLE, is_flagged BOOLEAN, reason_codes ARRAY<STRING>)
PARTITIONED BY (category STRING)
STORED AS PARQUET
LOCATION 's3://<bucket>/lake/gold/fact_review/';
-- MSCK REPAIR TABLE review_integrity.fact_review;
