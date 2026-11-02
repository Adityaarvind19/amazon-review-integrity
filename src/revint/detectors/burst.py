"""Review-burst detection: weekly review counts per product vs a trailing robust baseline.

For products with enough reviews, the weekly series is zero-filled between the first and
last review week. For each week the baseline is the median and IQR of the previous
`baseline_weeks` weeks (current week excluded, so a burst cannot mask itself):

    robust_z = (count - median) / max(IQR / 1.349, sqrt(median + 1))

The sqrt floor is a Poisson noise floor, which stops z from exploding on sparse series
where IQR = 0. A week is a burst if robust_z >= z_threshold and count >= min_count, with at
least `min_history_weeks` of history (launch weeks are a known blind spot, see README).
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def detect(reviews: DataFrame, cfg: dict) -> dict:
    b = cfg["burst"]
    r = reviews.select("category", "parent_asin", "review_id", "rating",
                       F.date_trunc("week", "review_ts").cast("date").alias("week"))
    eligible = (r.groupBy("category", "parent_asin").agg(F.count("*").alias("n"),
                                                         F.min("week").alias("w0"), F.max("week").alias("w1"))
                .filter(F.col("n") >= b["min_product_reviews"]))
    weeks = eligible.select("category", "parent_asin",
                            F.explode(F.sequence("w0", "w1", F.expr("INTERVAL 7 DAYS"))).alias("week"))
    counts = r.groupBy("category", "parent_asin", "week").agg(
        F.count("*").alias("n_reviews"),
        F.avg("rating").alias("avg_rating"),
        F.avg((F.col("rating") >= 4).cast("double")).alias("share_4_5"))
    series = (weeks.join(counts, ["category", "parent_asin", "week"], "left")
              .fillna({"n_reviews": 0}))
    w_hist = (Window.partitionBy("category", "parent_asin").orderBy("week")
              .rowsBetween(-b["baseline_weeks"], -1))
    w_idx = Window.partitionBy("category", "parent_asin").orderBy("week")
    q = F.percentile_approx(F.col("n_reviews").cast("double"),
                            F.array(F.lit(0.25), F.lit(0.5), F.lit(0.75)), 1000).over(w_hist)
    scored = (series.withColumn("q", q)
              .withColumn("week_idx", F.row_number().over(w_idx) - 1)
              .withColumn("baseline_median", F.col("q")[1])
              .withColumn("baseline_iqr", F.col("q")[2] - F.col("q")[0])
              .withColumn("sigma", F.greatest(F.col("baseline_iqr") / 1.349, F.sqrt(F.col("baseline_median") + 1)))
              .withColumn("robust_z", (F.col("n_reviews") - F.col("baseline_median")) / F.col("sigma"))
              .drop("q"))
    bursts = (scored.filter((F.col("week_idx") >= b["min_history_weeks"])
                            & (F.col("robust_z") >= b["z_threshold"])
                            & (F.col("n_reviews") >= b["min_count"]))
              .withColumn("excess_share", F.greatest(F.lit(0.0), 1 - F.col("baseline_median") / F.col("n_reviews")))
              .select("category", "parent_asin", "week", "n_reviews", "baseline_median", "baseline_iqr",
                      "robust_z", "excess_share", "avg_rating", "share_4_5")
              .localCheckpoint())
    members = (r.join(bursts.select("category", "parent_asin", "week", "robust_z", "excess_share"),
                      ["category", "parent_asin", "week"])
               .select("category", "review_id", F.col("robust_z").alias("burst_z"),
                       F.col("excess_share").alias("burst_excess_share")))
    stats = {"burst_eligible_products": eligible.count(), "burst_weeks": bursts.count(),
             "burst_products": bursts.select("category", "parent_asin").distinct().count()}
    return {"bursts": bursts, "members": members, "stats": stats}
