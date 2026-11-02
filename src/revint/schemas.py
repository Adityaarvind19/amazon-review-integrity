"""Explicit schemas for the McAuley Lab Amazon Reviews 2023 raw JSONL files.

Explicit schemas (instead of inference) make ingest a single pass, keep types stable across
categories and route malformed lines to `_corrupt_record` for quarantine.
"""
from pyspark.sql.types import ArrayType, BooleanType, DoubleType, LongType, StringType, StructField, StructType

REVIEW_IMAGE = StructType([
    StructField("small_image_url", StringType()),
    StructField("medium_image_url", StringType()),
    StructField("large_image_url", StringType()),
    StructField("attachment_type", StringType()),
])

REVIEW_SCHEMA = StructType([
    StructField("rating", DoubleType()),
    StructField("title", StringType()),
    StructField("text", StringType()),
    StructField("images", ArrayType(REVIEW_IMAGE)),
    StructField("asin", StringType()),
    StructField("parent_asin", StringType()),
    StructField("user_id", StringType()),
    StructField("timestamp", LongType()),          # Unix epoch milliseconds
    StructField("helpful_vote", LongType()),
    StructField("verified_purchase", BooleanType()),
    StructField("_corrupt_record", StringType()),
])

META_IMAGE = StructType([
    StructField("thumb", StringType()),
    StructField("large", StringType()),
    StructField("variant", StringType()),
    StructField("hi_res", StringType()),
])

META_SCHEMA = StructType([
    StructField("main_category", StringType()),
    StructField("title", StringType()),
    StructField("average_rating", DoubleType()),
    StructField("rating_number", LongType()),
    StructField("features", ArrayType(StringType())),
    StructField("description", ArrayType(StringType())),
    StructField("price", StringType()),            # float, null or the string "None" -> parsed in silver
    StructField("images", ArrayType(META_IMAGE)),
    StructField("store", StringType()),
    StructField("categories", ArrayType(StringType())),
    StructField("details", StringType()),          # heterogeneous dict -> kept as raw JSON text
    StructField("parent_asin", StringType()),
    StructField("bought_together", StringType()),
    StructField("_corrupt_record", StringType()),
])
