#!/bin/bash
# scripts/submit-spark-job.sh

set -e

APP_JAR="/app/target/parquet-stream-reader-1.0-SNAPSHOT.jar"
MAIN_CLASS="com.streaming.ParquetStreamReader"

echo "========================================="
echo "Building Spark Application..."
echo "========================================="

cd /app

# Clean and build
mvn clean package -DskipTests

if [ ! -f "$APP_JAR" ]; then
    echo "✗ Build failed! JAR file not found: $APP_JAR"
    exit 1
fi

echo "✓ Build successful!"
echo ""
echo "========================================="
echo "Submitting Spark job to YARN..."
echo "========================================="

spark-submit \
  --class $MAIN_CLASS \
  --master yarn \
  --deploy-mode client \
  --driver-memory 1g \
  --executor-memory 2g \
  --executor-cores 1 \
  --num-executors 2 \
  --conf spark.yarn.submit.waitAppCompletion=true \
  --conf spark.sql.streaming.schemaInference=true \
  --conf spark.sql.parquet.compression.codec=snappy \
  --conf spark.yarn.maxAppAttempts=1 \
  $APP_JAR

echo ""
echo "========================================="
echo "Spark job completed!"
echo "========================================="
