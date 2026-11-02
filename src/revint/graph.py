"""Connected components on Spark DataFrames (no GraphFrames dependency).

Min-label propagation with pointer jumping: each round, a node takes the minimum label
of itself and its neighbours, then jumps to its label's label. Pointer jumping makes the
number of rounds roughly logarithmic in component diameter instead of linear. The component
id is the minimum node id in the component, so output is deterministic.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def connected_components(edges: DataFrame, src: str = "src", dst: str = "dst",
                         max_iter: int = 60) -> tuple[DataFrame, int]:
    e = edges.select(F.col(src).alias("a"), F.col(dst).alias("b")).filter(F.col("a") != F.col("b"))
    e = e.union(e.select(F.col("b").alias("a"), F.col("a").alias("b"))).distinct().localCheckpoint()
    labels = (e.groupBy("a").agg(F.min("b").alias("m"))
              .select(F.col("a").alias("node"), F.least("a", "m").alias("label")).localCheckpoint())
    iterations = 0
    for iterations in range(1, max_iter + 1):  # noqa: B007 (value returned after loop)
        prop = (e.join(labels, e.b == labels.node)
                .select(F.col("a").alias("node"), "label")
                .unionByName(labels)
                .groupBy("node").agg(F.min("label").alias("label"))
                .localCheckpoint())
        parent = prop.select(F.col("node").alias("p_node"), F.col("label").alias("p_label"))
        jumped = (prop.join(parent, prop.label == parent.p_node, "left")
                  .select("node", F.least(F.col("label"), F.coalesce(F.col("p_label"), F.col("label"))).alias("label"))
                  .localCheckpoint())
        old = labels.select(F.col("node").alias("o_node"), F.col("label").alias("o_label"))
        changed = (jumped.join(old, jumped.node == old.o_node)
                   .filter(F.col("label") != F.col("o_label")).limit(1).count())
        labels = jumped
        if changed == 0:
            break
    return labels.withColumnRenamed("label", "component"), iterations
