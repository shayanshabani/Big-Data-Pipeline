#!/bin/bash
# hadoop/scripts/start-hdfs.sh

set -e

SERVICE=$1

echo "========================================="
echo "Starting service: $SERVICE"
echo "========================================="

wait_for_service() {
    local host=$1
    local port=$2
    local max_attempts=60
    local attempt=1
    
    echo "Waiting for $host:$port to be available..."
    
    while [ $attempt -le $max_attempts ]; do
        if nc -z $host $port 2>/dev/null; then
            echo "✓ $host:$port is available!"
            return 0
        fi
        echo "Attempt $attempt/$max_attempts: $host:$port not available yet..."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    echo "✗ Failed to connect to $host:$port after $max_attempts attempts"
    return 1
}

case $SERVICE in
    "namenode")
        echo "Checking NameNode data directory..."
        if [ ! -d "/hadoop/dfs/name/current" ]; then
            echo "Formatting NameNode..."
            hdfs namenode -format -force -nonInteractive
        else
            echo "NameNode already formatted. Skipping format."
        fi
        echo "Starting NameNode..."
        hdfs --daemon start namenode
        
        # Wait for NameNode to start
        sleep 5
        
        echo "NameNode started successfully!"
        echo "Web UI available at: http://localhost:9870"
        
        # Keep container running and tail logs
        tail -f /opt/hadoop/logs/hadoop-*-namenode-*.log 2>/dev/null || tail -f /dev/null
        ;;
        
    "datanode")
        wait_for_service namenode 9870
        echo "Starting DataNode..."
        hdfs --daemon start datanode
        
        echo "DataNode started successfully!"
        
        # Keep container running and tail logs
        tail -f /opt/hadoop/logs/hadoop-*-datanode-*.log 2>/dev/null || tail -f /dev/null
        ;;
        
    "resourcemanager")
        wait_for_service namenode 9870
        # wait_for_service datanode1 9864
        # wait_for_service datanode2 9864
        
        echo "Starting ResourceManager..."
        yarn --daemon start resourcemanager
        
        echo "ResourceManager started successfully!"
        echo "Web UI available at: http://localhost:8088"
        
        # Keep container running and tail logs
        tail -f /opt/hadoop/logs/yarn-*-resourcemanager-*.log 2>/dev/null || tail -f /dev/null
        ;;
        
    "nodemanager")
        wait_for_service namenode 9870
        wait_for_service resourcemanager 8088
        
        echo "Starting NodeManager..."
        yarn --daemon start nodemanager
        
        echo "NodeManager started successfully!"
        
        # Keep container running and tail logs
        tail -f /opt/hadoop/logs/yarn-*-nodemanager-*.log 2>/dev/null || tail -f /dev/null
        ;;

    "worker")
        wait_for_service namenode 9870
        wait_for_service resourcemanager 8088
        
        echo "Starting Worker Node..."
        hdfs --daemon start datanode

        echo "DataNode started successfully!"
        yarn --daemon start nodemanager
        
        echo "NodeManager started successfully!"
        DATANODE_LOG="/opt/hadoop/logs/hadoop-*-datanode-*.log"
        NODEMANAGER_LOG="/opt/hadoop/logs/yarn-*-nodemanager-*.log"

        # Wait for at least one log file to exist before tailing
        while [ ! -f $DATANODE_LOG ] && [ ! -f $NODEMANAGER_LOG ]; do
            echo "Waiting for log files to be created..."
            sleep 2
        done

        # Tail both log files. The '-F' flag is better than '-f' as it handles
        # log file rotation and recreation gracefully.
        tail -F $DATANODE_LOG $NODEMANAGER_LOG
        # Keep container running and tail logs
        # tail -f /opt/hadoop/logs/yarn-*-nodemanager-*.log 2>/dev/null || tail -f /dev/null
        ;;
        
    *)
        echo "Unknown service: $SERVICE"
        echo "Available services: namenode, datanode, resourcemanager, nodemanager"
        exit 1
        ;;
esac
