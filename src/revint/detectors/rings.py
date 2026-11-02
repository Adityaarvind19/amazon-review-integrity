"""Review rings: a lockstep co-review graph (in the spirit of Facebook's CopyCatch).

Two reviewers are linked when they reviewed >= `min_shared_products` of the same products,
each time within `window_days` of each other, AND those shared products make up at least
`min_lockstep_ratio` of the smaller reviewer's catalogue (this filters power reviewers who
overlap by chance on popular products). Connected components of that graph are rings.

Pair generation is bucketed by (product, window) so it never compares reviews far apart in
time; oversized buckets are skipped and counted so the cost stays bounded and visible.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..graph import connected_components


def detect(reviews: DataFrame, cfg: dict) -> dict:
    rc = cfg["rings"]
    W = rc["window_days"]
    ur = (reviews.groupBy("category", "user_id", "parent_asin")
          .agg(F.min(F.datediff("review_date", F.lit("1970-01-01"))).alias("day"))
          .withColumn("ukey", F.xxhash64("user_id")))
    degree = ur.groupBy("category", "ukey").agg(F.count("*").alias("deg"))
    # emit into own bucket and the next one: any pair with |d1-d2| <= W shares a bucket
    bucketed = (ur.withColumn("b0", F.floor(F.col("day") / W))
                .withColumn("bucket", F.explode(F.array(F.col("b0"), F.col("b0") + 1))))
    sizes = bucketed.groupBy("category", "parent_asin", "bucket").agg(F.count("*").alias("bsize"))
    ok = sizes.filter((F.col("bsize") >= 2) & (F.col("bsize") <= rc["max_bucket_size"]))
    skipped = sizes.filter(F.col("bsize") > rc["max_bucket_size"])
    bk = bucketed.join(ok.select("category", "parent_asin", "bucket"), ["category", "parent_asin", "bucket"])
    a = bk.select("category", "parent_asin", "bucket", F.col("ukey").alias("u1"), F.col("day").alias("d1"))
    b = bk.select("category", "parent_asin", "bucket", F.col("ukey").alias("u2"), F.col("day").alias("d2"))
    pairs = (a.join(b, ["category", "parent_asin", "bucket"])
             .filter((F.col("u1") < F.col("u2")) & (F.abs(F.col("d1") - F.col("d2")) <= W))
             .select("category", "u1", "u2", "parent_asin").distinct())
    edges = (pairs.groupBy("category", "u1", "u2").agg(F.count("*").alias("shared"))
             .filter(F.col("shared") >= rc["min_shared_products"])
             .join(degree.select("category", F.col("ukey").alias("u1"), F.col("deg").alias("deg1")), ["category", "u1"])
             .join(degree.select("category", F.col("ukey").alias("u2"), F.col("deg").alias("deg2")), ["category", "u2"])
             .withColumn("lockstep_ratio", F.col("shared") / F.least("deg1", "deg2"))
             .filter(F.col("lockstep_ratio") >= rc["min_lockstep_ratio"])
             .localCheckpoint())

    stats = {"ring_skipped_buckets": skipped.count(), "ring_edges": edges.count()}
    if stats["ring_edges"] == 0:
        stats.update(ring_cc_iterations=0, rings=0, giant_components=0)
        empty = reviews.limit(0)
        return {"rings": empty.select("category", F.lit(0).cast("long").alias("ring_id")),
                "members": empty.select("category", "review_id", F.lit(0).cast("long").alias("ring_id")),
                "stats": stats}

    comp, iters = connected_components(edges.select(F.col("u1").alias("src"), F.col("u2").alias("dst")))
    stats["ring_cc_iterations"] = iters
    node_cat = edges.select(F.col("u1").alias("node"), "category").unionByName(
        edges.select(F.col("u2").alias("node"), "category")).distinct()
    user_ring = comp.join(node_cat, "node").select("category", F.col("node").alias("ukey"),
                                                   F.col("component").alias("ring_id"))
    # products of a ring = products appearing in its lockstep pairs
    ring_products = (pairs.join(edges.select("category", "u1", "u2"), ["category", "u1", "u2"])
                     .join(user_ring.withColumnRenamed("ukey", "u1"), ["category", "u1"])
                     .select("category", "ring_id", "parent_asin").distinct())
    sizes_r = user_ring.groupBy("category", "ring_id").agg(F.count("*").alias("n_members"))
    edge_cnt = (edges.join(user_ring.withColumnRenamed("ukey", "u1"), ["category", "u1"])
                .groupBy("category", "ring_id").agg(F.count("*").alias("n_edges")))
    rev = reviews.select("category", "review_id", "user_id", "parent_asin", "rating", "review_ts",
                         "verified_purchase").withColumn("ukey", F.xxhash64("user_id"))
    ring_reviews = (rev.join(user_ring, ["category", "ukey"])
                    .join(ring_products, ["category", "ring_id", "parent_asin"]))
    rings = (ring_reviews.groupBy("category", "ring_id").agg(
                F.count("*").alias("n_reviews"),
                F.countDistinct("parent_asin").alias("n_products"),
                F.avg("rating").alias("avg_rating"),
                F.avg(F.col("verified_purchase").cast("double")).alias("verified_share"),
                F.min("review_ts").alias("first_ts"), F.max("review_ts").alias("last_ts"))
             .join(sizes_r, ["category", "ring_id"]).join(edge_cnt, ["category", "ring_id"])
             .withColumn("density", F.col("n_edges") / (F.col("n_members") * (F.col("n_members") - 1) / 2))
             .withColumn("is_giant", F.col("n_members") > rc["max_ring_size"])
             .localCheckpoint())
    members = (ring_reviews.join(rings.filter(~F.col("is_giant")).select("category", "ring_id"), ["category", "ring_id"])
               .select("category", "review_id", "ring_id"))
    stats["rings"] = rings.filter(~F.col("is_giant")).count()
    stats["giant_components"] = rings.filter(F.col("is_giant")).count()
    return {"rings": rings, "members": members, "stats": stats}
