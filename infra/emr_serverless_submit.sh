#!/usr/bin/env bash
# UNTESTED SKETCH (not executed): run the pipeline on EMR Serverless.
set -euo pipefail
APP_ID=${APP_ID:?EMR Serverless application id}
ROLE_ARN=${ROLE_ARN:?job execution role}
BUCKET=${BUCKET:?s3 bucket}
python -m build --wheel && aws s3 cp dist/revint-0.1.0-py3-none-any.whl "s3://$BUCKET/code/"
aws s3 cp config/pipeline.toml "s3://$BUCKET/code/"
aws emr-serverless start-job-run --application-id "$APP_ID" --execution-role-arn "$ROLE_ARN" \
  --job-driver "{\"sparkSubmit\": {\"entryPoint\": \"s3://$BUCKET/code/entry.py\",
     \"entryPointArguments\": [\"--raw-dir\", \"s3a://$BUCKET/raw\", \"--lake-dir\", \"s3a://$BUCKET/lake\", \"run\"],
     \"sparkSubmitParameters\": \"--conf spark.archives=s3://$BUCKET/code/venv.tar.gz#env --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./env/bin/python\"}}"
