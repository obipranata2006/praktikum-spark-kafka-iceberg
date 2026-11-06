#!/usr/bin/env bash
# Menjalankan scripts/04_spark_pipeline.py lewat spark-submit.
# Contoh:
#   bash scripts/run_spark.sh --run-id smoke-a --shuffle-partitions 8 --salt-buckets 16 --pause
set -euo pipefail
cd /app

SPARK_SUBMIT="${SPARK_HOME:-/opt/spark}/bin/spark-submit"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-labadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-labpassword123}"

PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1"
PACKAGES+=",org.apache.iceberg:iceberg-aws-bundle:1.7.1"
PACKAGES+=",org.apache.hadoop:hadoop-aws:3.3.4"
PACKAGES+=",com.amazonaws:aws-java-sdk-bundle:1.12.262"

exec "$SPARK_SUBMIT" \
  --master 'local[4]' \
  --driver-memory "${SPARK_DRIVER_MEMORY:-3g}" \
  --packages "$PACKAGES" \
  --conf spark.jars.ivy=/root/.ivy2 \
  --conf spark.ui.port=4040 \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.sql.autoBroadcastJoinThreshold=-1 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lake.type=rest \
  --conf spark.sql.catalog.lake.uri=http://rest:8181 \
  --conf spark.sql.catalog.lake.warehouse=s3://warehouse/ \
  --conf spark.sql.catalog.lake.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.lake.s3.endpoint="$MINIO_ENDPOINT" \
  --conf spark.sql.catalog.lake.s3.path-style-access=true \
  --conf spark.sql.catalog.lake.client.region=us-east-1 \
  --conf spark.hadoop.fs.s3a.endpoint="$MINIO_ENDPOINT" \
  --conf spark.hadoop.fs.s3a.access.key="$MINIO_ACCESS_KEY" \
  --conf spark.hadoop.fs.s3a.secret.key="$MINIO_SECRET_KEY" \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  scripts/04_spark_pipeline.py "$@"
