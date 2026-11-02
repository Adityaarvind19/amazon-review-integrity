"""Gold layer: star schema (fact_review + dims) and analytical marts."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import lake
from .silver import INCENTIVE_BAN_DATE
from .spark import write_by_category, write_table

FACT_COLS = ["category", "review_id", "date_key", "product_key", "reviewer_key", "review_ts", "review_year",
             "rating", "verified_purchase", "helpful_vote", "has_images", "n_tokens", "sentiment",
             "polar_tokens", "is_incentivized", "is_mismatch", "dup_cluster_id", "cluster_type", "ring_id",
             "burst_z", "user_reviews_same_day", "x_near_dup", "x_copy_paste", "x_ring", "x_burst",
             "x_mismatch", "x_incentivized", "x_unverified", "x_same_day", "suspicion", "is_flagged",
             "reason_codes"]


def build_fact_and_dim_product(spark: SparkSession, cfg: dict, categories: list[str], scored: DataFrame,
                               products: DataFrame) -> None:
    fact = (scored.withColumn("date_key", F.date_format("review_date", "yyyyMMdd").cast("int"))
            .select(*FACT_COLS))
    write_by_category(spark, fact.sortWithinPartitions("product_key", "review_ts"),
                      lake(cfg, "gold", "fact_review"), categories)
    # inferred members: products reviewed but absent from metadata keep FK integrity
    inferred = (scored.select("category", "product_key", "parent_asin").distinct()
                .join(products.select("category", "product_key"), ["category", "product_key"], "left_anti")
                .select("category", "product_key", "parent_asin",
                        F.lit(None).cast("string").alias("product_title"), F.lit(None).cast("string").alias("main_category"),
                        F.lit("UNKNOWN").alias("store"), F.lit("UNKNOWN").alias("brand"),
                        F.lit(None).cast("double").alias("price"), F.lit(None).cast("double").alias("listed_avg_rating"),
                        F.lit(None).cast("long").alias("listed_rating_count"), F.lit(None).cast("string").alias("sub_category"),
                        F.lit(0).alias("n_images"), F.lit(True).alias("is_inferred_member")))
    dim_product = (products.withColumn("is_inferred_member", F.lit(False)).unionByName(inferred)
                   .withColumn("price_band",
                               F.when(F.col("price").isNull(), "unknown").when(F.col("price") < 10, "<$10")
                               .when(F.col("price") < 25, "$10-25").when(F.col("price") < 50, "$25-50")
                               .when(F.col("price") < 100, "$50-100").otherwise("$100+")))
    write_by_category(spark, dim_product, lake(cfg, "gold", "dim_product"), categories, num_files=1)


def build_global_dims(spark: SparkSession, cfg: dict) -> None:
    """Small conformed dimensions rebuilt from the whole fact table (all categories)."""
    fact = spark.read.parquet(lake(cfg, "gold", "fact_review"))
    silver = spark.read.parquet(lake(cfg, "silver", "reviews")).select("reviewer_key", "user_id").distinct()
    per_day = fact.groupBy("reviewer_key", "date_key").agg(F.count("*").alias("n"))
    dim_reviewer = (fact.groupBy("reviewer_key").agg(
                        F.min("review_ts").alias("first_review_ts"), F.max("review_ts").alias("last_review_ts"),
                        F.count("*").alias("n_reviews"), F.countDistinct("category").alias("n_categories"),
                        F.avg((F.col("rating") == 5).cast("double")).alias("five_star_share"),
                        F.avg(F.col("verified_purchase").cast("double")).alias("verified_share"),
                        F.sum(F.col("is_flagged").cast("int")).alias("n_flagged"),
                        F.max(F.col("ring_id").isNotNull()).alias("is_ring_member"))
                    .join(per_day.groupBy("reviewer_key").agg(F.max("n").alias("max_reviews_one_day")), "reviewer_key")
                    .join(silver, "reviewer_key", "left"))
    write_table(dim_reviewer, lake(cfg, "gold", "dim_reviewer"), num_files=4)
    # format inside Spark (session TZ = UTC): collecting timestamps into Python would convert them
    # to the laptop's local zone and could shift the first/last date by a day
    bounds = fact.agg(F.date_format(F.min("review_ts"), "yyyy-MM-dd").alias("lo"),
                      F.date_format(F.max("review_ts"), "yyyy-MM-dd").alias("hi")).first()
    dates = spark.sql(f"SELECT explode(sequence(to_date('{bounds['lo']}'), "
                      f"to_date('{bounds['hi']}'), interval 1 day)) AS date")
    dim_date = dates.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"), "date",
        F.year("date").alias("year"), F.quarter("date").alias("quarter"), F.month("date").alias("month"),
        F.date_format("date", "yyyy-MM").alias("year_month"), F.weekofyear("date").alias("iso_week"),
        F.dayofweek("date").alias("day_of_week"),
        (F.col("date") >= F.lit(INCENTIVE_BAN_DATE).cast("date")).alias("is_post_incentive_ban"))
    write_table(dim_date, lake(cfg, "gold", "dim_date"), num_files=1)
    cats = fact.groupBy("category").agg(F.count("*").alias("n_reviews"))
    write_table(cats.select(F.xxhash64("category").alias("category_key"), "category", "n_reviews"),
                lake(cfg, "gold", "dim_category"), num_files=1)


def build_marts(spark: SparkSession, cfg: dict, categories: list[str], scored: DataFrame, product_mart: DataFrame,
                products: DataFrame, dup: dict, rings: dict, burst: dict) -> None:
    pm = product_mart.join(products.select("category", "product_key", "product_title", "store", "brand", "price",
                                           "listed_avg_rating", "listed_rating_count"),
                           ["category", "product_key"], "left")
    write_by_category(spark, pm, lake(cfg, "gold", "mart_product_integrity"), categories, num_files=2)

    store = (pm.filter(F.col("store") != "UNKNOWN").groupBy("category", "store").agg(
        F.count("*").alias("n_products"), F.sum("n_reviews").alias("n_reviews"),
        F.sum("n_flagged").alias("n_flagged"),
        (F.sum(F.col("integrity_score") * F.col("n_reviews")) / F.sum("n_reviews")).alias("weighted_integrity"),
        F.avg("rating_inflation").alias("avg_rating_inflation"),
        F.sum((F.col("integrity_grade").isin("D", "F")).cast("int")).alias("n_low_integrity_products"))
        .withColumn("flagged_share", F.col("n_flagged") / F.col("n_reviews")))
    write_by_category(spark, store, lake(cfg, "gold", "mart_store_integrity"), categories, num_files=1)

    monthly = (scored.withColumn("year_month", F.date_format("review_date", "yyyy-MM"))
               .groupBy("category", "year_month").agg(
                   F.count("*").alias("n_reviews"),
                   F.avg(F.col("is_flagged").cast("double")).alias("flagged_share"),
                   F.avg("rating").alias("raw_avg_rating"),
                   F.avg(F.when(~F.col("is_flagged"), F.col("rating"))).alias("trusted_avg_rating"),
                   F.avg(F.col("is_incentivized").cast("double")).alias("incentivized_share"),
                   F.avg(F.col("verified_purchase").cast("double")).alias("verified_share"),
                   F.avg(F.col("x_near_dup")).alias("near_dup_share"),
                   F.avg(F.col("x_ring")).alias("ring_share"),
                   F.avg(F.col("burst_z").isNotNull().cast("double")).alias("burst_share"),
                   F.avg(F.col("is_mismatch").cast("double")).alias("mismatch_share")))
    write_by_category(spark, monthly, lake(cfg, "gold", "mart_category_monthly"), categories, num_files=1)

    base = scored.select("category", "review_id", "user_id", "parent_asin", "rating", "review_ts", "verified_purchase")

    def summarize(members: DataFrame, id_col: str, ctype) -> DataFrame:
        return (members.join(base, ["category", "review_id"]).groupBy("category", id_col).agg(
            F.count("*").alias("n_reviews"), F.countDistinct("user_id").alias("n_reviewers"),
            F.countDistinct("parent_asin").alias("n_products"), F.avg("rating").alias("avg_rating"),
            F.avg(F.col("verified_purchase").cast("double")).alias("verified_share"),
            F.min("review_ts").alias("first_ts"), F.max("review_ts").alias("last_ts"),
            F.min("parent_asin").alias("example_parent_asin"))
            .withColumn("campaign_type", ctype)
            .withColumn("campaign_id", F.col(id_col).cast("long")).drop(id_col))

    dup_m = dup["members"].join(dup["clusters"].filter(F.col("cluster_type") != "MINOR")
                                .select("category", "dup_cluster_id", "cluster_type"), ["category", "dup_cluster_id"])
    c1 = summarize(dup_m.select("category", "review_id", "dup_cluster_id", "cluster_type"),
                   "dup_cluster_id", F.lit("NEAR_DUP")).drop("cluster_type")
    c1 = c1.join(dup["clusters"].select("category", F.col("dup_cluster_id").alias("campaign_id"),
                                        F.col("cluster_type").alias("subtype")), ["category", "campaign_id"])
    c2 = summarize(rings["members"], "ring_id", F.lit("LOCKSTEP_RING")).withColumn("subtype", F.lit("RING"))
    bm = burst["members"].join(base.select("category", "review_id", "parent_asin"), ["category", "review_id"]) \
        .join(scored.select("category", "review_id", F.date_trunc("week", "review_ts").cast("date").alias("week")),
              ["category", "review_id"]) \
        .withColumn("burst_id", F.xxhash64("parent_asin", "week")).select("category", "review_id", "burst_id")
    c3 = summarize(bm, "burst_id", F.lit("REVIEW_BURST")).withColumn("subtype", F.lit("WEEKLY_BURST"))
    campaigns = c1.unionByName(c2).unionByName(c3)
    write_by_category(spark, campaigns, lake(cfg, "gold", "mart_campaigns"), categories, num_files=1)
