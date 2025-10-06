#!/bin/bash
# scripts/submit-spark-job.sh

set -e

APP_JAR="target/spark-streaming-hdfs-1.0-SNAPSHOT.jar"
MAIN_CLASS="com.streaming.core.StreamingJobSubmitter"

echo "========================================="
echo "Building Spark Application..."
echo "========================================="

cd spark/app

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

sudo docker cp ${APP_JAR} spark-client:/app/

sudo docker exec -it spark-client spark-submit \
  --class $MAIN_CLASS \
  --master yarn \
  --deploy-mode client \
  --driver-memory 512m \
  --executor-memory 1g \
  --executor-cores 1 \
  --num-executors 2 \
  --conf "spark.sql.parquet.enableVectorizedReader=false" \
  --conf "spark.sql.parquet.int96AsTimestamp=true" \
  $APP_JAR \
  sales-aggregation

echo ""
echo "========================================="
echo "Spark job completed!"
echo "========================================="
