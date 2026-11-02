"""Near-duplicate review text at scale: MinHash LSH -> star edges -> exact Jaccard -> clusters.

Instead of materialising all pairs inside an LSH bucket (quadratic for popular buckets),
every bucket member is linked to the bucket's anchor (its minimum review_id). Candidate
edges are therefore linear in bucket size, then each is verified with exact shingle Jaccard,
and connected components turn verified edges into duplicate clusters.
"""
from __future__ import annotations

import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf

from .. import textfeat
from ..graph import connected_components


def _jaccard_udf(k: int):
    @pandas_udf("double")
    def jac(a: pd.Series, b: pd.Series) -> pd.Series:
        return pd.Series([textfeat.jaccard(textfeat.tokenize(x), textfeat.tokenize(y), k)
                          for x, y in zip(a, b)])
    return jac


def detect(reviews: DataFrame, text_feats: DataFrame, cfg: dict) -> dict:
    nd = cfg["near_dup"]
    base = reviews.select("category", "review_id", "user_id", "parent_asin", "review_ts", "rating",
                          "verified_purchase", "text_clean")
    banded = (text_feats.filter(F.col("lsh_bands").isNotNull())
              .select("category", "review_id", F.posexplode("lsh_bands").alias("band", "bucket")))
    buckets = (banded.groupBy("category", "band", "bucket")
               .agg(F.count("*").alias("bucket_size"), F.min("review_id").alias("anchor"))
               .filter("bucket_size >= 2"))
    candidates = (banded.join(buckets, ["category", "band", "bucket"])
                  .filter(F.col("review_id") != F.col("anchor"))
                  .select("category", F.col("review_id").alias("src"), F.col("anchor").alias("dst"))
                  .distinct())
    txt = base.select("category", "review_id", "text_clean")
    jac = _jaccard_udf(nd["shingle_size"])
    verified = (candidates
                .join(txt.withColumnRenamed("review_id", "src").withColumnRenamed("text_clean", "t1"), ["category", "src"])
                .join(txt.withColumnRenamed("review_id", "dst").withColumnRenamed("text_clean", "t2"), ["category", "dst"])
                .withColumn("jaccard", jac("t1", "t2"))
                .filter(F.col("jaccard") >= nd["jaccard_threshold"])
                .select("category", "src", "dst", "jaccard")
                .localCheckpoint())

    stats = {"lsh_eligible_reviews": text_feats.filter(F.col("lsh_bands").isNotNull()).count(),
             "lsh_candidate_edges": candidates.count(),
             "verified_edges": verified.count()}
    n = stats["lsh_eligible_reviews"]
    stats["naive_pairs"] = n * (n - 1) // 2
    stats["pair_reduction_factor"] = (stats["naive_pairs"] / stats["lsh_candidate_edges"]
                                      if stats["lsh_candidate_edges"] else None)

    if stats["verified_edges"] == 0:
        empty_members = base.limit(0).select(
            "category", "review_id", F.lit(0).cast("long").alias("dup_cluster_id"),
            F.lit(0).alias("users_same_product"), F.lit(0).alias("products_same_user"),
            F.lit(False).alias("dup_campaign"), F.lit(False).alias("dup_copy_paste"))
        clusters = empty_members.select("category", "dup_cluster_id").limit(0)
        stats["cc_iterations"] = 0
        return {"edges": verified, "members": empty_members, "clusters": _cluster_stats(empty_members, base, nd),
                "stats": stats}

    # review ids are category-local hashes; components run per category through the key
    comp, iters = connected_components(verified.select("src", "dst"))
    stats["cc_iterations"] = iters
    cat_of = verified.select(F.col("src").alias("node"), "category").unionByName(
        verified.select(F.col("dst").alias("node"), "category")).distinct()
    members = (comp.join(cat_of, "node")
               .select("category", F.col("node").alias("review_id"), F.col("component").alias("dup_cluster_id")))
    members = _member_flags(members, base, nd).localCheckpoint()
    clusters = _cluster_stats(members, base, nd)
    return {"edges": verified, "members": members, "clusters": clusters, "stats": stats}


def _member_flags(members: DataFrame, base: DataFrame, nd: dict) -> DataFrame:
    """Per-review flags that are robust to transitive chaining inside large clusters:
    a review is part of a templated campaign only if >= min_users distinct reviewers posted
    near-identical text on the SAME product within +/-15 days of it, or the whole cluster is
    >= min_users reviewers packed into <= 30 days. Organic boilerplate (e.g. the pre-2016
    "received at a discount in exchange for my honest review" text) chains into large clusters
    spread over years and is therefore not counted. Copy-paste = one reviewer, same text,
    >= N products."""
    j = members.join(base.select("category", "review_id", "user_id", "parent_asin", "review_ts"),
                     ["category", "review_id"])
    cp = (Window.partitionBy("category", "dup_cluster_id", "parent_asin")
          .orderBy(F.unix_seconds("review_ts")).rangeBetween(-15 * 86400, 15 * 86400))
    cu = Window.partitionBy("category", "dup_cluster_id", "user_id")
    cl = Window.partitionBy("category", "dup_cluster_id")
    span_days = (F.max(F.unix_seconds("review_ts")).over(cl) - F.min(F.unix_seconds("review_ts")).over(cl)) / 86400
    j = (j.withColumn("users_same_product", F.size(F.collect_set("user_id").over(cp)))
          .withColumn("products_same_user", F.size(F.collect_set("parent_asin").over(cu)))
          .withColumn("cluster_users", F.size(F.collect_set("user_id").over(cl)))
          .withColumn("cluster_span_days", span_days))
    return j.select(
        "category", "review_id", "dup_cluster_id", "users_same_product", "products_same_user",
        ((F.col("users_same_product") >= nd["min_users_campaign"])
         | ((F.col("cluster_users") >= nd["min_users_campaign"]) & (F.col("cluster_span_days") <= 30)))
        .alias("dup_campaign"),
        (F.col("products_same_user") >= nd["min_products_copy_paste"]).alias("dup_copy_paste"))


def _cluster_stats(members: DataFrame, base: DataFrame, nd: dict) -> DataFrame:
    j = members.join(base.drop("text_clean"), ["category", "review_id"])
    agg = j.groupBy("category", "dup_cluster_id").agg(
        F.count("*").alias("n_reviews"),
        F.countDistinct("user_id").alias("n_users"),
        F.countDistinct("parent_asin").alias("n_products"),
        F.min("review_ts").alias("first_ts"), F.max("review_ts").alias("last_ts"),
        F.avg("rating").alias("avg_rating"),
        F.avg(F.col("verified_purchase").cast("double")).alias("verified_share"),
        F.sum(F.col("dup_campaign").cast("int")).alias("n_campaign_reviews"),
        F.sum(F.col("dup_copy_paste").cast("int")).alias("n_copy_paste_reviews"))
    return agg.withColumn(
        "cluster_type",
        F.when(F.col("n_campaign_reviews") > 0, "TEMPLATE_CAMPAIGN")
        .when(F.col("n_copy_paste_reviews") > 0, "COPY_PASTE")
        .otherwise("MINOR"))
