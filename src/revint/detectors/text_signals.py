"""One vectorised pass over review text: tokens count, lexicon sentiment, incentivised
disclosure, and MinHash LSH band keys. Implemented as a pandas UDF over Arrow batches
(a pure Spark-SQL higher-order-function MinHash was ~50x slower in testing)."""
from __future__ import annotations

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, BooleanType, DoubleType, IntegerType, LongType, StructField, StructType

from .. import textfeat

TEXT_STRUCT = StructType([
    StructField("n_tokens", IntegerType()),
    StructField("sentiment", DoubleType()),
    StructField("polar_tokens", IntegerType()),
    StructField("incentivized", BooleanType()),
    StructField("lsh_bands", ArrayType(LongType())),
])


def make_udf(nd: dict):
    k, num_perm, bands = nd["shingle_size"], nd["num_perm"], nd["bands"]
    min_sh, seed = nd["min_shingles"], nd["seed"]
    if num_perm % bands:
        raise ValueError("num_perm must be divisible by bands")

    @pandas_udf(TEXT_STRUCT)
    def text_features(text: pd.Series, title: pd.Series) -> pd.DataFrame:
        cols = textfeat.text_features_batch(text.tolist(), title.tolist(), k=k, num_perm=num_perm,
                                            bands=bands, min_shingles=min_sh, seed=seed)
        return pd.DataFrame(cols)

    return text_features


def compute(reviews: DataFrame, cfg: dict) -> DataFrame:
    udf = make_udf(cfg["near_dup"])
    return (reviews.select("category", "review_id", "text_clean", "title_clean")
            .withColumn("tf", udf("text_clean", "title_clean"))
            .select("category", "review_id", "tf.*"))
