from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def get_spark(cfg: dict, app: str = "review-integrity") -> SparkSession:
    sc = cfg["spark"]
    # pin the driver's Python process to UTC as well, so local datetimes never depend on the laptop's zone
    os.environ["TZ"] = "UTC"
    time.tzset()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    ckpt = Path(cfg["paths"]["state_dir"], "spark-checkpoints")
    ckpt.mkdir(parents=True, exist_ok=True)
    spark = (
        SparkSession.builder.appName(app)
        .master(sc["master"])
        .config("spark.driver.memory", sc["driver_memory"])
        .config("spark.sql.shuffle.partitions", str(sc["shuffle_partitions"]))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "zstd")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", str(sc.get("arrow_batch_size", 5000)))
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.enabled", os.environ.get("REVINT_SPARK_UI", "false"))
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.setCheckpointDir(str(ckpt))
    return spark


def _fs(spark: SparkSession, path: str):
    jvm = spark.sparkContext._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    return hpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), hpath


def delete_path(spark: SparkSession, path: str) -> None:
    fs, hpath = _fs(spark, path)
    if fs.exists(hpath):
        fs.delete(hpath, True)


def path_exists(spark: SparkSession, path: str) -> bool:
    fs, hpath = _fs(spark, path)
    return fs.exists(hpath)


def path_size_bytes(spark: SparkSession, path: str) -> int:
    fs, hpath = _fs(spark, path)
    return int(fs.getContentSummary(hpath).getLength()) if fs.exists(hpath) else 0


def write_by_category(spark: SparkSession, df: DataFrame, path: str, categories: list[str],
                      extra_partitions: tuple[str, ...] = (), num_files: int | None = None) -> None:
    """Idempotent category-scoped overwrite: delete category=<c> partitions, then append.

    Re-running a category always reproduces exactly the same partition contents; other
    categories already in the table are untouched (this is what makes loads incremental).
    """
    for c in categories:
        delete_path(spark, f"{path}/category={c}")
    if num_files:
        df = df.repartition(num_files)
    (df.write.mode("append").partitionBy("category", *extra_partitions).parquet(path))


def write_table(df: DataFrame, path: str, num_files: int | None = None) -> None:
    """Full overwrite for small global tables (dimensions)."""
    if num_files:
        df = df.coalesce(num_files)
    df.write.mode("overwrite").parquet(path)


def read_categories(spark: SparkSession, path: str, categories: list[str]) -> DataFrame:
    df = spark.read.option("basePath", path).parquet(*[f"{path}/category={c}" for c in categories])
    return df


def read_categories_or_none(spark: SparkSession, path: str, categories: list[str]) -> DataFrame | None:
    """Like read_categories, but returns None when no category partition exists (empty detector output)."""
    present = [c for c in categories if path_exists(spark, f"{path}/category={c}")]
    if not present:
        return None
    return read_categories(spark, path, present)
