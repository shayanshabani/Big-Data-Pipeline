#!/bin/bash
# scripts/create-directories.sh

set -e

echo "========================================="
echo "Creating HDFS directories..."
echo "========================================="

# Wait for NameNode to be ready
echo "Waiting for NameNode to be ready..."
for i in {1..30}; do
    if hdfs dfsadmin -report &>/dev/null; then
        echo "✓ NameNode is ready!"
        break
    fi
    echo "Attempt $i/30: Waiting for NameNode..."
    sleep 5
done

# Create input directory for streaming data
echo "Creating /streaming/sales/input..."
hdfs dfs -mkdir -p /streaming/sales/input

echo "Creating /streaming/sales/checkpoint..."
hdfs dfs -mkdir -p /streaming/sales/checkpoint

echo "Creating /references..."
hdfs dfs -mkdir -p /references

# Create Spark logs directory
echo "Creating /spark-logs..."
hdfs dfs -mkdir -p /spark-logs

# Create output directory (for future use)
echo "Creating /output..."
hdfs dfs -mkdir -p /output

# Set permissions
echo "Setting permissions..."
hdfs dfs -chmod -R 777 /streaming/sales/input
hdfs dfs -chmod -R 777 /spark-logs
hdfs dfs -chmod -R 777 /output
hdfs dfs -chmod -R 777 /streaming/sales/checkpoint
hdfs dfs -chmod -R 777 /references

echo ""
echo "✓ HDFS directories created successfully!"
echo ""
echo "Directory structure:"
hdfs dfs -ls /
echo ""
echo "========================================="

echo ""
echo "Copying reference files"
echo ""
hdfs dfs -put /tmp/SQM_2GCell.csv /references/
hdfs dfs -put /tmp/SQM_4GCell.csv /references/
echo ""
echo "✓ Reference file copied successfully!"
echo ""
echo "References structure:"
hdfs dfs -ls /references
echo ""
echo "========================================="
