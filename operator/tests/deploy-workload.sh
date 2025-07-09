#!/bin/bash

# --- Configuration ---
KAFKA_BROKERS="my-cluster-kafka-bootstrap:9092"  # Replace with your Kafka broker list
TOPIC_NAME="my-topic-1"
#MESSAGE_SIZE_BYTES=1024  # Message size in bytes
#THROUGHPUT_PER_PRODUCER=1000  # Messages per second per producer
#TEST_DURATION_SECONDS=60  # Duration of the test in seconds
START=$(date)
OUTPUT_FILE="kafka_performance_results_$(date +%Y%m%d_%H%M%S).txt"
NUM_RECORDS=1000

# --- Helper Functions ---

if [ -z "$NUM_CONSUMERS" ] ; then
  NUM_CONSUMERS=1
fi

if [ -z "$NUM_PRODUCERS" ] ; then
  NUM_PRODUCERS=1
fi

if [ -z "$THROUGHPUT_PER_PRODUCER" ] ; then
  THROUGHPUT_PER_PRODUCER=-1
fi

if [ -z "$MESSAGE_SIZE_BYTES" ] ; then
  MESSAGE_SIZE_BYTES=1024
fi

function start_producers() {
  echo "Starting $NUM_PRODUCERS producers..."
  for i in $(seq 1 "$NUM_PRODUCERS"); do
    echo "Starting producer $i..."
    "kafka-producer-perf-test" \
      --producer-props bootstrap.servers=my-cluster-kafka-bootstrap:9092 \
      --topic "$TOPIC_NAME" \
      --num-records "$NUM_RECORDS" \
      --record-size "$MESSAGE_SIZE_BYTES" \
      --throughput "$THROUGHPUT_PER_PRODUCER" \
      > "producer_${i}_output.log" 2>&1 &
    producer_pids+=("$!")
  done
  echo "All producers started with PIDs: ${producer_pids[*]}"
}

function start_consumers() {
  echo "Starting $NUM_CONSUMERS consumers..."
  for i in $(seq 1 "$NUM_CONSUMERS"); do
    echo "Starting consumer group consumer_$i..."
    "kafka-consumer-perf-test" \
      --bootstrap-server "$KAFKA_BROKERS" \
      --topic "$TOPIC_NAME" \
      --num-messages "$(($NUM_PRODUCERS * $NUM_RECORDS))" \
      --group consumer_$i \
      --fetch-size 1048576 \
      --print-metrics \
      > "consumer_${i}_output.log" 2>&1 &
    consumer_pids+=("$!")
  done
  echo "All consumers started with PIDs: ${consumer_pids[*]}"
}

function wait_for_test() {
  # wait for all the producers to terminate
    for id in "${producer_ids[@]}" ; do
    echo "waiting for $id"
    wait $id
    done

    # kill all the consumers
    for id in "${consumer_ids[@]}" ; do
    echo "killing $id"
    kill -INT $id
    done
}



function stop_processes() {
  echo "Stopping producers..."
  for pid in "${producer_pids[@]}"; do
    echo "Stopping producer with PID: $pid"
    kill "$pid" 2>/dev/null
  done

  echo "Stopping consumers..."
  for pid in "${consumer_pids[@]}"; do
    echo "Stopping consumer with PID: $pid"
    kill "$pid" 2>/dev/null
  done
}

function collect_results() {
  echo "Collecting results..."
  echo "Start Test: $START" >> "$OUTPUT_FILE"
  END=$(date)
  echo "End Test: $END" >> "$OUTPUT_FILE"
  echo "Num Producers: $NUM_PRODUCERS" >> "$OUTPUT_FILE"
  echo "Num Consumers: $NUM_CONSUMERS" >> "$OUTPUT_FILE"
  echo "Message Size: $MESSAGE_SIZE_BYTES" >> "$OUTPUT_FILE"
  echo "Prod Throughput: $THROUGHPUT_PER_PRODUCER" >> "$OUTPUT_FILE"
  echo "--- Producer Performance ---" >> "$OUTPUT_FILE"

  for i in $(seq 1 "$NUM_PRODUCERS"); do
    echo "Producer $i logs:" >> "$OUTPUT_FILE"
    cat "producer_${i}_output.log" >> "$OUTPUT_FILE"
    rm "producer_${i}_output.log"
  done

  echo "" >> "$OUTPUT_FILE"
  echo "--- Consumer Performance ---" >> "$OUTPUT_FILE"
  for i in $(seq 1 "$NUM_CONSUMERS"); do
    echo "Consumer group consumer_$i logs:" >> "$OUTPUT_FILE"
    cat "consumer_${i}_output.log" >> "$OUTPUT_FILE"
    rm "consumer_${i}_output.log"
  done

  echo "" >> "$OUTPUT_FILE"
  echo "Performance test results saved to: $OUTPUT_FILE"
}

# --- Main Script ---

# Check if KAFKA_HOME is set
if [ -z "$KAFKA_HOME" ]; then
    KAFKA_HOME="."
fi

# Initialize arrays to store process IDs
producer_pids=()
consumer_pids=()

start_producers
start_consumers

wait_for_test


collect_results


echo "Script finished."