"""Review-level suspicion (explainable noisy-OR) and product-level integrity / trusted rating."""
from __future__ import annotations

from functools import reduce

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

SIGNALS = [  # (column, weight key, reason code)
    ("x_near_dup", "w_near_dup_campaign", "NEAR_DUP_CAMPAIGN"),
    ("x_copy_paste", "w_copy_paste", "COPY_PASTE"),
    ("x_ring", "w_ring", "LOCKSTEP_RING"),
    ("x_burst", "w_burst", "REVIEW_BURST"),
    ("x_mismatch", "w_mismatch", "RATING_TEXT_MISMATCH"),
    ("x_incentivized", "w_incentivized", "INCENTIVIZED_DISCLOSURE"),
    ("x_unverified", "w_unverified", "UNVERIFIED"),
    ("x_same_day", "w_same_day", "SAME_DAY_SPREE"),
]


def noisy_or(cols_weights: list[tuple[Column, float]]) -> Column:
    keep = reduce(lambda acc, cw: acc * (1 - F.lit(cw[1]) * cw[0]), cols_weights, F.lit(1.0))
    return 1 - keep


def score_reviews(reviews: DataFrame, text: DataFrame, dup: dict, rings: dict, burst: dict,
                  cfg: dict) -> DataFrame:
    s, sc = cfg["signals"], cfg["scoring"]
    dup_m = (dup["members"].join(dup["clusters"].select("category", "dup_cluster_id", "cluster_type"),
                                 ["category", "dup_cluster_id"]))
    user_day = Window.partitionBy("category", "user_id", "review_date")
    user_all = Window.partitionBy("category", "user_id")
    df = (reviews.drop("text_clean", "title_clean")
          .join(text.drop("lsh_bands"), ["category", "review_id"], "left")
          .join(dup_m, ["category", "review_id"], "left")
          .join(rings["members"].dropDuplicates(["category", "review_id"]), ["category", "review_id"], "left")
          .join(burst["members"], ["category", "review_id"], "left")
          .withColumn("user_reviews_same_day", F.count("*").over(user_day))
          .withColumn("user_reviews_in_category", F.count("*").over(user_all)))
    mismatch = (((F.col("rating") >= s["mismatch_pos_rating"]) & (F.col("sentiment") <= s["mismatch_neg_sentiment"]))
                | ((F.col("rating") <= s["mismatch_neg_rating"]) & (F.col("sentiment") >= s["mismatch_pos_sentiment"]))
                ) & (F.col("polar_tokens") >= s["min_polar_tokens"])
    extreme = (F.col("rating") >= 4) | (F.col("rating") <= 2)
    df = (df
          .withColumn("is_mismatch", F.coalesce(mismatch, F.lit(False)))
          .withColumn("is_incentivized", F.coalesce(F.col("incentivized"), F.lit(False)))
          .withColumn("x_near_dup", F.col("dup_campaign").cast("double"))
          .withColumn("x_copy_paste", (F.col("dup_copy_paste") & ~F.col("dup_campaign")).cast("double"))
          .withColumn("x_ring", F.col("ring_id").isNotNull().cast("double"))
          .withColumn("x_burst", F.when(F.col("burst_z").isNotNull() & extreme, F.col("burst_excess_share")))
          .withColumn("x_mismatch", F.col("is_mismatch").cast("double"))
          .withColumn("x_incentivized", F.col("is_incentivized").cast("double"))
          .withColumn("x_unverified", (~F.col("verified_purchase")).cast("double"))
          .withColumn("x_same_day", (F.col("user_reviews_same_day") >= s["same_day_reviews"]).cast("double"))
          .fillna(0.0, subset=[c for c, _, _ in SIGNALS]))
    df = df.withColumn("suspicion", noisy_or([(F.col(c), sc[w]) for c, w, _ in SIGNALS]))
    reasons = F.array_compact(F.array(*[F.when(F.col(c) > 0, F.lit(code)) for c, _, code in SIGNALS]))
    return (df.withColumn("reason_codes", reasons)
              .withColumn("is_flagged", F.col("suspicion") >= sc["flag_threshold"]))


def product_integrity(scored: DataFrame, cfg: dict) -> DataFrame:
    m = float(cfg["scoring"]["prior_strength"])
    cat = (scored.filter(~F.col("is_flagged")).groupBy("category")
           .agg(F.avg("rating").alias("category_trusted_mean")))
    p = (scored.groupBy("category", "parent_asin", "product_key").agg(
            F.count("*").alias("n_reviews"),
            F.sum(F.col("is_flagged").cast("int")).alias("n_flagged"),
            F.avg("suspicion").alias("mean_suspicion"),
            F.avg("rating").alias("raw_avg_rating"),
            F.sum("rating").alias("_sum_all"),
            F.sum(F.when(~F.col("is_flagged"), F.col("rating"))).alias("_sum_trusted"),
            F.sum((~F.col("is_flagged")).cast("int")).alias("n_trusted"),
            F.sum(F.col("x_near_dup").cast("int")).alias("n_near_dup_campaign"),
            F.sum(F.col("x_ring").cast("int")).alias("n_ring_reviews"),
            F.sum((F.col("burst_z").isNotNull()).cast("int")).alias("n_burst_reviews"),
            F.sum(F.col("is_incentivized").cast("int")).alias("n_incentivized"),
            F.sum(F.col("is_mismatch").cast("int")).alias("n_mismatch"),
            F.avg(F.col("verified_purchase").cast("double")).alias("verified_share"),
            F.min("review_date").alias("first_review_date"), F.max("review_date").alias("last_review_date"))
         .join(cat, "category"))
    mu = F.col("category_trusted_mean")
    p = (p.withColumn("flagged_share", F.col("n_flagged") / F.col("n_reviews"))
         .withColumn("trusted_mean_rating", F.col("_sum_trusted") / F.col("n_trusted"))
         .withColumn("raw_bayes_rating", (F.col("_sum_all") + m * mu) / (F.col("n_reviews") + m))
         .withColumn("trust_adjusted_rating",
                     (F.coalesce(F.col("_sum_trusted"), F.lit(0.0)) + m * mu) / (F.col("n_trusted") + m))
         .withColumn("rating_inflation", F.col("raw_bayes_rating") - F.col("trust_adjusted_rating"))
         .withColumn("integrity_score", F.round(F.greatest(F.lit(0.0), F.least(F.lit(100.0),
                     100 * (1 - 0.7 * F.col("flagged_share") - 0.3 * F.col("mean_suspicion")))), 2))
         .withColumn("integrity_grade",
                     F.when(F.col("integrity_score") >= 90, "A").when(F.col("integrity_score") >= 75, "B")
                     .when(F.col("integrity_score") >= 60, "C").when(F.col("integrity_score") >= 40, "D")
                     .otherwise("F"))
         .withColumn("low_confidence", F.col("n_reviews") < 10)
         .drop("_sum_all", "_sum_trusted"))
    w = Window.partitionBy("category").orderBy(F.col("integrity_score").asc(), F.col("n_reviews").desc(),
                                               F.col("parent_asin"))
    return (p.withColumn("integrity_rank_worst_first", F.row_number().over(w))
             .withColumn("integrity_percentile", F.round(F.percent_rank().over(
                 Window.partitionBy("category").orderBy("integrity_score")) * 100, 2)))
