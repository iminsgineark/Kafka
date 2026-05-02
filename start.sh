#!/bin/bash

KAFKA_DIR=$(pwd)
LOG_DIR="/tmp/kafka-logs"
DATA_DIR="/tmp/kraft-combined-logs"   # default from server.properties
CLUSTER_ID_FILE="$KAFKA_DIR/cluster.id"

echo "Starting Kafka..."

# Create log directory
mkdir -p $LOG_DIR

# Check if Kafka already running
if lsof -i :9092 > /dev/null
then
    echo "Kafka already running on port 9092"
    exit 1
fi

# Step 1: Generate Cluster ID (only once)
if [ ! -f "$CLUSTER_ID_FILE" ]; then
    echo "Generating Cluster ID..."
    CLUSTER_ID=$($KAFKA_DIR/bin/kafka-storage.sh random-uuid)
    echo $CLUSTER_ID > $CLUSTER_ID_FILE
else
    CLUSTER_ID=$(cat $CLUSTER_ID_FILE)
fi

# Step 2: Format storage (only if not already formatted)
if [ ! -f "$DATA_DIR/meta.properties" ]; then
    echo "Formatting storage directory..."
    $KAFKA_DIR/bin/kafka-storage.sh format \
        -t $CLUSTER_ID \
        -c $KAFKA_DIR/config/kraft/server.properties
else
    echo "Storage already formatted"
fi

# Step 3: Start Kafka
nohup $KAFKA_DIR/bin/kafka-server-start.sh \
    $KAFKA_DIR/config/kraft/server.properties \
    > $LOG_DIR/kafka.log 2>&1 &

echo "Kafka started in background"
echo "Logs: $LOG_DIR/kafka.log"