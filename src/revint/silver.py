"""Silver layer: typed, validated, de-duplicated reviews and products.

Every bronze row ends up in exactly one place: silver, quarantine (with a reason), or the
exact-duplicate count. The DQ gate reconciles silver + quarantine + duplicates == bronze.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .config import lake
from .spark import read_categories, write_by_category

MIN_TS_MS = 801_964_800_000          # 1995-06-01: Amazon's first reviews
INCENTIVE_BAN_DATE = "2016-10-03"    # Amazon banned incentivised reviews (outside Vine)


def review_id_col() -> Column:
    """Deterministic 64-bit surrogate key (the dataset has no review id)."""
    return F.xxhash64(F.col("user_id"), F.col("parent_asin"), F.col("asin"),
                      F.col("timestamp"), F.coalesce(F.col("text"), F.lit("")))


def key_col(name: str) -> Column:
    return F.xxhash64(F.col(name))


def clean_text_col(c: Column) -> Column:
    c = F.regexp_replace(c, r"<[^>]*>", " ")
    c = F.regexp_replace(c, r"&#?[a-zA-Z0-9]+;", " ")
    return F.trim(F.regexp_replace(c, r"\s+", " "))


def build_reviews(spark: SparkSession, cfg: dict, categories: list[str]) -> dict:
    bronze = read_categories(spark, lake(cfg, "bronze", "reviews"), categories)
    now_ms = F.lit(int(__import__("time").time() * 1000) + 86_400_000)
    reason = (
        F.when(F.col("_corrupt_record").isNotNull(), "MALFORMED_JSON")
        .when(F.col("user_id").isNull() | (F.trim("user_id") == ""), "MISSING_USER_ID")
        .when(F.col("parent_asin").isNull() | (F.trim("parent_asin") == ""), "MISSING_PARENT_ASIN")
        .when(F.col("rating").isNull() | ~F.col("rating").isin(1.0, 2.0, 3.0, 4.0, 5.0), "BAD_RATING")
        .when(F.col("timestamp").isNull() | (F.col("timestamp") < MIN_TS_MS) | (F.col("timestamp") > now_ms),
              "BAD_TIMESTAMP")
    )
    staged = bronze.withColumn("_dq_reason", reason)
    staged.cache()
    quarantine = (staged.filter(F.col("_dq_reason").isNotNull())
                  .select("category", "_source_file", "_dq_reason", "_corrupt_record", "user_id",
                          "parent_asin", "rating", "timestamp"))
    valid = staged.filter(F.col("_dq_reason").isNull()).withColumn("review_id", review_id_col())

    # exact duplicates: same review_id (user, product, variant, timestamp, text). Keep the
    # copy with the most helpful votes; tie-break on all remaining columns for determinism.
    w = Window.partitionBy("category", "review_id").orderBy(
        F.col("helpful_vote").desc_nulls_last(), F.col("verified_purchase").desc_nulls_last(),
        F.col("title").asc_nulls_last(), F.col("_source_file"))
    ranked = valid.withColumn("_rn", F.row_number().over(w))
    deduped = ranked.filter("_rn = 1")

    reviews = (deduped
               .withColumn("review_ts", F.timestamp_millis("timestamp"))
               .withColumn("review_date", F.to_date("review_ts"))
               .withColumn("review_year", F.year("review_ts"))
               .withColumn("text_clean", clean_text_col(F.coalesce("text", F.lit(""))))
               .withColumn("title_clean", clean_text_col(F.coalesce("title", F.lit(""))))
               .withColumn("has_images", F.coalesce(F.size("images") > 0, F.lit(False)))
               .withColumn("helpful_vote", F.coalesce("helpful_vote", F.lit(0)))
               .withColumn("verified_purchase", F.coalesce("verified_purchase", F.lit(False)))
               .withColumn("product_key", key_col("parent_asin"))
               .withColumn("reviewer_key", key_col("user_id"))
               .select("category", "review_year", "review_id", "product_key", "reviewer_key",
                       "user_id", "parent_asin", "asin", "rating", "timestamp", "review_ts",
                       "review_date", "title_clean", "text_clean", "has_images", "helpful_vote",
                       "verified_purchase"))

    write_by_category(spark, reviews.sortWithinPartitions("parent_asin", "timestamp"),
                      lake(cfg, "silver", "reviews"), categories, extra_partitions=("review_year",))
    write_by_category(spark, quarantine, lake(cfg, "quarantine", "reviews"), categories, num_files=1)

    stats = (staged.groupBy("category").agg(
        F.count("*").alias("bronze_rows"),
        F.sum(F.col("_dq_reason").isNotNull().cast("int")).alias("quarantined")).collect())
    silver_counts = {r["category"]: r["n"] for r in spark.read.parquet(lake(cfg, "silver", "reviews"))
                     .filter(F.col("category").isin(categories)).groupBy("category")
                     .agg(F.count("*").alias("n")).collect()}
    reasons = {(r["category"], r["_dq_reason"]): r["n"] for r in quarantine.groupBy("category", "_dq_reason")
               .agg(F.count("*").alias("n")).collect()}
    staged.unpersist()
    out = {}
    for r in stats:
        c = r["category"]
        valid_n = r["bronze_rows"] - r["quarantined"]
        out[c] = {"bronze_rows": r["bronze_rows"], "quarantined": r["quarantined"],
                  "duplicates_removed": valid_n - silver_counts.get(c, 0),
                  "silver_rows": silver_counts.get(c, 0),
                  "quarantine_reasons": {k[1]: v for k, v in reasons.items() if k[0] == c}}
    return out


def _price(c: Column) -> Column:
    s = F.regexp_replace(F.trim(c), r"[$,]", "")
    return F.when(s.rlike(r"^[0-9]+(\.[0-9]+)?$"), s.try_cast("double"))


def build_products(spark: SparkSession, cfg: dict, categories: list[str]) -> dict:
    bronze = read_categories(spark, lake(cfg, "bronze", "products"), categories)
    valid = bronze.filter(F.col("_corrupt_record").isNull() & F.col("parent_asin").isNotNull())
    w = Window.partitionBy("category", "parent_asin").orderBy(
        F.col("rating_number").desc_nulls_last(), F.col("title").asc_nulls_last())
    brand = F.coalesce(F.get_json_object("details", "$.Brand"), F.get_json_object("details", "$.brand"),
                       F.col("store"))
    products = (valid.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
                .select("category",
                        F.xxhash64("parent_asin").alias("product_key"),
                        "parent_asin",
                        F.col("title").alias("product_title"),
                        "main_category",
                        F.coalesce(F.trim("store"), F.lit("UNKNOWN")).alias("store"),
                        F.coalesce(F.trim(brand), F.lit("UNKNOWN")).alias("brand"),
                        _price(F.col("price")).alias("price"),
                        F.col("average_rating").alias("listed_avg_rating"),
                        F.col("rating_number").alias("listed_rating_count"),
                        F.element_at(F.col("categories"), 2).alias("sub_category"),
                        F.coalesce(F.size("images"), F.lit(0)).alias("n_images")))
    write_by_category(spark, products, lake(cfg, "silver", "products"), categories, num_files=1)
    rows = {r["category"]: r["n"] for r in bronze.groupBy("category").agg(F.count("*").alias("n")).collect()}
    kept = {r["category"]: r["n"] for r in spark.read.parquet(lake(cfg, "silver", "products"))
            .filter(F.col("category").isin(categories)).groupBy("category").agg(F.count("*").alias("n")).collect()}
    return {c: {"bronze_rows": rows.get(c, 0), "silver_rows": kept.get(c, 0)} for c in categories}


def silver_reviews(spark: SparkSession, cfg: dict, categories: list[str]) -> DataFrame:
    return read_categories(spark, lake(cfg, "silver", "reviews"), categories)


def silver_products(spark: SparkSession, cfg: dict, categories: list[str]) -> DataFrame:
    return read_categories(spark, lake(cfg, "silver", "products"), categories)
