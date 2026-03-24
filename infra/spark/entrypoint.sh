#!/usr/bin/env bash

set -euo pipefail

MODE="${MODE:-incremental}"
REFRESH_DAYS="${REFRESH_DAYS:-7}"
JOB_SCRIPT="${JOB_SCRIPT:-infra/spark/jobs/build_processing.py}"
PIPELINE="${PIPELINE:-}"
SPARK_MASTER="${SPARK_MASTER:-local[4]}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-6g}"
SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-6g}"
SPARK_SQL_SHUFFLE_PARTITIONS="${SPARK_SQL_SHUFFLE_PARTITIONS:-512}"
SPARK_DEFAULT_PARALLELISM="${SPARK_DEFAULT_PARALLELISM:-256}"

required_vars=(
  CATALOG_URI
  WAREHOUSE
  R2_CATALOG_TOKEN
  R2_ENDPOINT
  R2_ACCESS_KEY_ID
  R2_SECRET_ACCESS_KEY
)

for var_name in "${required_vars[@]}"; do
  if [[ -z "${!var_name:-}" ]]; then
    echo "Missing required env var: ${var_name}" >&2
    exit 1
  fi
done

if [[ "$MODE" != "incremental" && "$MODE" != "full" ]]; then
  echo "Invalid MODE: ${MODE}. Expected incremental|full." >&2
  exit 1
fi

if [[ -n "$PIPELINE" && "$PIPELINE" != "feature-serving" && "$PIPELINE" != "ml" && "$PIPELINE" != "all" ]]; then
  echo "Invalid PIPELINE: ${PIPELINE}. Expected feature-serving|ml|all." >&2
  exit 1
fi

if [[ ! -f "$JOB_SCRIPT" ]]; then
  echo "Spark job script does not exist: ${JOB_SCRIPT}" >&2
  exit 1
fi

job_args=(--mode "${MODE}" --refresh-days "${REFRESH_DAYS}")
if [[ -n "$PIPELINE" ]]; then
  job_args+=(--pipeline "${PIPELINE}")
fi

SPARK_SUBMIT_BIN="${SPARK_SUBMIT_BIN:-}"
if [[ -z "$SPARK_SUBMIT_BIN" ]]; then
  if command -v spark-submit >/dev/null 2>&1; then
    SPARK_SUBMIT_BIN="$(command -v spark-submit)"
  elif [[ -x "/opt/spark/bin/spark-submit" ]]; then
    SPARK_SUBMIT_BIN="/opt/spark/bin/spark-submit"
  elif [[ -x "/opt/bitnami/spark/bin/spark-submit" ]]; then
    SPARK_SUBMIT_BIN="/opt/bitnami/spark/bin/spark-submit"
  fi
fi

if [[ -z "$SPARK_SUBMIT_BIN" ]]; then
  echo "Unable to find spark-submit. Set SPARK_SUBMIT_BIN explicitly." >&2
  exit 1
fi

exec "$SPARK_SUBMIT_BIN" \
  --master "${SPARK_MASTER}" \
  --driver-memory "${SPARK_DRIVER_MEMORY}" \
  --conf "spark.executor.memory=${SPARK_EXECUTOR_MEMORY}" \
  --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:bundle:2.20.160,software.amazon.awssdk:url-connection-client:2.20.160" \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.iceberg.vectorization.enabled=false" \
  --conf "spark.sql.parquet.enableVectorizedReader=false" \
  --conf "spark.sql.parquet.enableNestedColumnVectorizedReader=false" \
  --conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS}" \
  --conf "spark.default.parallelism=${SPARK_DEFAULT_PARALLELISM}" \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
  --conf "spark.sql.defaultCatalog=tf2" \
  --conf "spark.sql.catalog.tf2=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.tf2.type=rest" \
  --conf "spark.sql.catalog.tf2.uri=${CATALOG_URI}" \
  --conf "spark.sql.catalog.tf2.warehouse=${WAREHOUSE}" \
  --conf "spark.sql.catalog.tf2.token=${R2_CATALOG_TOKEN}" \
  --conf "spark.sql.catalog.tf2.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.endpoint=${R2_ENDPOINT}" \
  --conf "spark.hadoop.fs.s3a.access.key=${R2_ACCESS_KEY_ID}" \
  --conf "spark.hadoop.fs.s3a.secret.key=${R2_SECRET_ACCESS_KEY}" \
  --conf "spark.hadoop.fs.s3a.region=auto" \
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=true" \
  "${JOB_SCRIPT}" \
  "${job_args[@]}"
