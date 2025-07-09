#!/bin/bash

# Configuration
KAFKA_BROKERS="my-cluster-kafka-bootstrap:9092"  # Replace with your Kafka broker list (e.g., localhost:9092)
TOPIC_NAME="my-topic-1"      # Replace with the topic you want to test
MESSAGE_SIZE=1024                 # Message size in bytes
NUM_MESSAGES=100                # Number of messages to send per test
PRODUCER_SCRIPT="kafka-producer-perf-test" # Assuming this script is in your PATH or current directory
INSTANCE_LIMIT=3
SPAWN_INTERVAL=60                 # Interval in seconds

echo "Starting Kafka Producer Performance Test Spawner..."

instance_count=0

while [ "$instance_count" -lt "$INSTANCE_LIMIT" ]; do
  instance_count=$((instance_count + 1))
  timestamp=$(date +%Y%m%d_%H%M%S)
  log_file="/tmp/producer_perf_test_${timestamp}_instance${instance_count}.log"

  echo "Spawning instance $instance_count at $(date) and logging to $log_file..."

  # Run the Kafka producer performance test in the background and redirect output to a log file
  nohup "$PRODUCER_SCRIPT" \
    --topic "$TOPIC_NAME" \
    --message-size "$MESSAGE_SIZE" \
    --num-messages "$NUM_MESSAGES" \
    --producer-props bootstrap.servers=my-cluster-kafka-bootstrap:9092 \
    > "$log_file" 2>&1 &

  echo "Instance $instance_count spawned with PID $!."

  if [ "$instance_count" -lt "$INSTANCE_LIMIT" ]; then
    echo "Waiting $SPAWN_INTERVAL seconds before spawning the next instance..."
    sleep "$SPAWN_INTERVAL"
  fi
done

echo "Reached the limit of $INSTANCE_LIMIT instances. Spawning complete."