#!/bin/bash

# Test Kafka consumer
KAFKA_BROKER="kafka-1:19092"
TOPIC="streaming-output"

echo "Consuming messages from topic: ${TOPIC}"
docker exec -it kafka-1 kafka-console-consumer.sh \
  --bootstrap-server ${KAFKA_BROKER} \
  --topic ${TOPIC} \
  --from-beginning \
  --property print.key=true \
  --property key.separator=" : " \
  --max-messages 100
