-- UNTESTED SKETCH (not executed): Redshift star schema for the gold layer + COPY from S3 Parquet.
CREATE TABLE dim_date (date_key INT PRIMARY KEY, date DATE, year SMALLINT, quarter SMALLINT, month SMALLINT,
  year_month CHAR(7), iso_week SMALLINT, day_of_week SMALLINT, is_post_incentive_ban BOOLEAN) DISTSTYLE ALL SORTKEY(date_key);

CREATE TABLE dim_product (product_key BIGINT PRIMARY KEY, category VARCHAR(64), parent_asin VARCHAR(16),
  product_title VARCHAR(1024), main_category VARCHAR(128), store VARCHAR(256), brand VARCHAR(256), price DOUBLE PRECISION,
  price_band VARCHAR(16), listed_avg_rating DOUBLE PRECISION, listed_rating_count BIGINT, sub_category VARCHAR(256),
  n_images INT, is_inferred_member BOOLEAN) DISTSTYLE ALL;

CREATE TABLE dim_reviewer (reviewer_key BIGINT PRIMARY KEY, user_id VARCHAR(64), first_review_ts TIMESTAMP,
  last_review_ts TIMESTAMP, n_reviews BIGINT, n_categories INT, five_star_share DOUBLE PRECISION,
  verified_share DOUBLE PRECISION, n_flagged BIGINT, is_ring_member BOOLEAN, max_reviews_one_day BIGINT)
  DISTSTYLE AUTO;

CREATE TABLE fact_review (review_id BIGINT, category VARCHAR(64), date_key INT REFERENCES dim_date(date_key),
  product_key BIGINT REFERENCES dim_product(product_key), reviewer_key BIGINT REFERENCES dim_reviewer(reviewer_key),
  review_ts TIMESTAMP, rating DOUBLE PRECISION, verified_purchase BOOLEAN, helpful_vote BIGINT, has_images BOOLEAN,
  n_tokens INT, sentiment DOUBLE PRECISION, is_incentivized BOOLEAN, is_mismatch BOOLEAN, dup_cluster_id BIGINT,
  ring_id BIGINT, burst_z DOUBLE PRECISION, suspicion DOUBLE PRECISION, is_flagged BOOLEAN, reason_codes SUPER)
  DISTKEY(product_key) SORTKEY(date_key);

CREATE TABLE mart_product_integrity (category VARCHAR(64), product_key BIGINT, parent_asin VARCHAR(16), n_reviews BIGINT,
  n_flagged BIGINT, raw_avg_rating DOUBLE PRECISION, trust_adjusted_rating DOUBLE PRECISION,
  rating_inflation DOUBLE PRECISION, integrity_score DOUBLE PRECISION, integrity_grade CHAR(1))
  DISTKEY(product_key) SORTKEY(integrity_score);

-- COPY fact_review FROM 's3://<bucket>/lake/gold/fact_review/' IAM_ROLE '<role-arn>' FORMAT AS PARQUET;
