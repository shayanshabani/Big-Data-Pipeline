#!/bin/bash

# Wait for Kafka to be ready
echo "Waiting for Kafka cluster to be ready..."
sleep 30

# Kafka broker address
KAFKA_BROKER="kafka-1:19092"

# Create streaming output topic with 6 partitions and replication factor 3
echo "Creating topic: streaming-output"
docker exec -it kafka-1 kafka-topics.sh \
  --create \
  --topic streaming-output \
  --bootstrap-server ${KAFKA_BROKER} \
  --partitions 6 \
  --replication-factor 1 \
  --config min.insync.replicas=1 \
  --config compression.type=lz4 \
  --config retention.ms=604800000 \
  --config segment.bytes=1073741824 \
  --config max.message.bytes=10485760

# Create additional topics as needed
echo "Creating topic: streaming-errors"
docker exec -it kafka-1 kafka-topics.sh \
  --create \
  --topic streaming-errors \
  --bootstrap-server ${KAFKA_BROKER} \
  --partitions 3 \
  --replication-factor 1 \
  --config min.insync.replicas=1

# List all topics
echo "Listing all topics:"
docker exec -it kafka-1 kafka-topics.sh \
  --list \
  --bootstrap-server ${KAFKA_BROKER}

# Describe streaming-output topic
echo "Topic details:"
docker exec -it kafka-1 kafka-topics.sh \
  --describe \
  --topic streaming-output \
  --bootstrap-server ${KAFKA_BROKER}

echo "Kafka topics created successfully!"
