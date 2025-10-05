#!/bin/bash

set -e

echo "Starting Kafka in KRaft mode..."

# Wait for other brokers to be ready
sleep 10

# Generate cluster ID if not exists
if [ ! -f /var/lib/kafka/data/cluster-id ]; then
    CLUSTER_ID=${CLUSTER_ID:-$(${KAFKA_HOME}/bin/kafka-storage.sh random-uuid)}
    echo ${CLUSTER_ID} > /var/lib/kafka/data/cluster-id
    echo "Generated Cluster ID: ${CLUSTER_ID}"
else
    CLUSTER_ID=$(cat /var/lib/kafka/data/cluster-id)
    echo "Using existing Cluster ID: ${CLUSTER_ID}"
fi

# Create server.properties from environment variables
cat > ${KAFKA_HOME}/config/kraft/server.properties << EOF
# Node and Process Configuration
node.id=${KAFKA_NODE_ID}
process.roles=${KAFKA_PROCESS_ROLES}
controller.quorum.voters=${KAFKA_CONTROLLER_QUORUM_VOTERS}

# Listeners
listeners=${KAFKA_LISTENERS}
advertised.listeners=${KAFKA_ADVERTISED_LISTENERS}
listener.security.protocol.map=${KAFKA_LISTENER_SECURITY_PROTOCOL_MAP}
controller.listener.names=${KAFKA_CONTROLLER_LISTENER_NAMES}
inter.broker.listener.name=${KAFKA_INTER_BROKER_LISTENER_NAME}

# Log Directories
log.dirs=/var/lib/kafka/data

# Replication Settings
offsets.topic.replication.factor=${KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR:-3}
transaction.state.log.replication.factor=${KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR:-3}
transaction.state.log.min.isr=${KAFKA_TRANSACTION_STATE_LOG_MIN_ISR:-2}
default.replication.factor=${KAFKA_DEFAULT_REPLICATION_FACTOR:-3}
min.insync.replicas=${KAFKA_MIN_INSYNC_REPLICAS:-2}

# Partition Settings
num.partitions=${KAFKA_NUM_PARTITIONS:-6}
num.network.threads=8
num.io.threads=16

# Log Retention
log.retention.hours=${KAFKA_LOG_RETENTION_HOURS:-168}
log.segment.bytes=${KAFKA_LOG_SEGMENT_BYTES:-1073741824}
log.retention.check.interval.ms=${KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS:-300000}

# Performance Tuning
compression.type=${KAFKA_COMPRESSION_TYPE:-lz4}
max.request.size=${KAFKA_MAX_REQUEST_SIZE:-10485760}
message.max.bytes=${KAFKA_MESSAGE_MAX_BYTES:-10485760}
replica.fetch.max.bytes=${KAFKA_REPLICA_FETCH_MAX_BYTES:-10485760}

# Socket Settings
socket.send.buffer.bytes=${KAFKA_SOCKET_SEND_BUFFER_BYTES:-1048576}
socket.receive.buffer.bytes=${KAFKA_SOCKET_RECEIVE_BUFFER_BYTES:-1048576}
socket.request.max.bytes=${KAFKA_SOCKET_REQUEST_MAX_BYTES:-104857600}

# Group Coordinator Settings
group.initial.rebalance.delay.ms=3000

# Auto Create Topics
auto.create.topics.enable=true
EOF

# Format storage if not already formatted
if [ ! -f /var/lib/kafka/data/meta.properties ]; then
    echo "Formatting Kafka storage directory..."
    ${KAFKA_HOME}/bin/kafka-storage.sh format \
        -t ${CLUSTER_ID} \
        -c ${KAFKA_HOME}/config/kraft/server.properties
else
    echo "Storage already formatted, skipping..."
fi

# Start Kafka
echo "Starting Kafka server..."
exec ${KAFKA_HOME}/bin/kafka-server-start.sh ${KAFKA_HOME}/config/kraft/server.properties
