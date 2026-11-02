# AWS mapping (design — not deployed)

**Status: everything in this repository has run locally (Spark `local[*]` on a laptop). Nothing has been deployed
to AWS.** This page maps each local component to the AWS service that would replace it, and lists the code or config
change each would need. The files in `infra/` are untested sketches.

| Layer / concern | Local implementation | AWS equivalent | Change required | Deployed? |
|---|---|---|---|---|
| Raw landing | `data/raw/*.jsonl.gz` | S3 `s3://<bucket>/raw/category=<c>/` | Set `paths.raw_dir` to an `s3a://` URI; the download script runs `aws s3 cp` | No |
| Bronze / silver / features / gold | Parquet, partitioned by `category` (silver also by `review_year`) | S3 prefixes with the same layout | `paths.lake_dir = "s3a://<bucket>/lake"`. `spark.py` already uses the Hadoop FileSystem API, so deleting and overwriting partitions works the same on S3. | No |
| Spark jobs | `python -m revint run` on local Spark | EMR Serverless (or EMR on EC2, or Glue 4/5 Spark jobs) | Package `src/` as a wheel, pass `--config` and paths as job args; see `infra/emr_serverless_submit.sh` | No |
| Incremental trigger | SHA-256 manifest in `data/state/` | S3 `ObjectCreated` event → Lambda → start an EMR Serverless job run; manifest kept in S3 or DynamoDB | Move manifest I/O behind an interface (S3 or DynamoDB backend) | No |
| Catalog | none (paths only) | Glue Data Catalog tables over the gold prefixes | Glue crawler or the DDL in `infra/athena_ddl.sql` | No |
| SQL analytics | DuckDB over Parquet (`sql/*.sql`) | Athena (Trino), or Redshift Spectrum | Minor dialect changes: `QUALIFY` → subquery, `GROUP BY ALL` → explicit columns, `unnest` → `CROSS JOIN UNNEST` | No |
| Warehouse serving | gold Parquet | Redshift: `COPY` from Parquet into a star schema | `infra/redshift_ddl.sql` (DISTKEY/SORTKEY choices explained inline) | No |
| Dashboard | static HTML (matplotlib) | QuickSight on Athena or Redshift | — | No |
| Orchestration | `scripts/run_real.sh` | Step Functions (or MWAA/Airflow): ingest → silver → DQ → detectors → gold → DQ | Each stage is already a function in `pipeline.py` | No |
| Data-quality gate | `quality.py`; the run fails on any error-level check | Same code inside the job; failure → Step Functions catch → SNS alert | — | No |
| Observability | JSON run manifest | CloudWatch metrics from the manifest; S3 copy of the manifests | — | No |
| Atomic table commits | delete-then-append per category partition (fine for local single-writer use) | Apache Iceberg tables in the Glue catalog (`MERGE`/`INSERT OVERWRITE`, snapshots, time travel) | Swap the Parquet writer for the Iceberg catalog | No |

## Why these choices

- **EMR Serverless over Glue jobs:** the code is plain PySpark with a pandas UDF and pinned NumPy/pyarrow versions.
  EMR Serverless accepts a custom image or a venv archive, so those versions carry over unchanged.
- **Redshift distribution:** `fact_review` uses `DISTKEY(product_key)` because the heaviest joins and aggregations
  are per product (integrity mart, burst lookups), and `SORTKEY(date_key)` for time-range scans. Small dimensions use
  `DISTSTYLE ALL`.
- **Iceberg:** on S3 the local "delete partition then append" write is not atomic. Iceberg's snapshot commits make
  the category overwrite atomic and keep incremental reruns idempotent under concurrent readers.

## Rough cost note (not measured)

No AWS bill exists for this project, so no cost figures are claimed here.
