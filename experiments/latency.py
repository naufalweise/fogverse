import requests # pip install requests

MBEAN_PRODUCE_REMOTETIME = "kafka.network:type=RequestMetrics,name= ,request=Produce"

def get_jmx_metric(jolokia_url, mbean, attribute=None):
    response = requests.get(f"{jolokia_url}/read/{mbean}", timeout=5)
    data = response.json()
    return data.get("value")

def measure_replication_latency(num_brokers, base_kafka_port, test_topic_name, replication_factor, num_messages, message_size_kb):    

    leader_metrics_found = False
    print("\n--- Replication Latency (RemoteTimeMs for Produce Requests) ---")
    for i, jolokia_url in enumerate(jolokia_urls):
        metrics = get_jmx_metric(jolokia_url, MBEAN_PRODUCE_REMOTETIME)
        
        if metrics:
            count = metrics.get("Count", 0)
            one_min_rate = metrics.get("OneMinuteRate", 0.0)

            # Heuristic: if this broker processed produce requests recently, its Count will be higher
            # or OneMinuteRate will be non-zero.
            # For a fresh test, the leader of the single partition topic should have a count >= num_messages
            if count > 0 or one_min_rate > 0.01 : # Check if there's substantial activity
                leader_metrics_found = True
                print(f"  Latency Statistics (ms):")
                print(f"    Mean  : {metrics.get('Mean', 'N/A'):.3f}")

import concurrent.futures
import re
import subprocess
import time

from experiments.constants import BROKER_ADDRESS, CLUSTER_ID, NUM_RECORDS, TOPIC_NAME
from experiments.utils.cleanup import cleanup
from experiments.utils.cluster_setup import setup_experiment_env
from experiments.utils.generate_payload import generate_payload
from experiments.utils.run_cmd import run_cmd
from fogverse.logger.fog import FogLogger

logger = FogLogger(f"throughput_{int(time.time())}")

def run_producer_test(num_records=NUM_RECORDS):
    # Run the Kafka producer performance test using payload.txt and track progress.
    cmd = (
        "kafka/bin/kafka-producer-perf-test.sh "
        f"--topic {TOPIC_NAME} "
        f"--num-records {num_records} "
        "--payload-file experiments/payload.txt "
        "--throughput -1 "
        f"--producer-props acks=all bootstrap.servers={BROKER_ADDRESS}"
    )
    logger.log_all(f"Running producer performance test with {num_records} records...")

    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    output = []

    running_total = 0
    for line in process.stdout:
        line = line.strip()
        output.append(line)

        match = re.match(r"(\d+)\s+records sent", line)
        if match:
            this_count = int(match.group(1))

            if this_count == num_records:
                logger.log_all("100% progress completed.")
            else:
                running_total += this_count
                percent = int((running_total / num_records) * 100)
                logger.log_all(f"{percent}% progress completed.")

    process.wait()
    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, cmd)

    return "\n".join(output)

def run_consumer_test(num_records=NUM_RECORDS):
    # Run the Kafka consumer performance test.
    cmd = (
        "kafka/bin/kafka-consumer-perf-test.sh "
        f"--topic {TOPIC_NAME} "
        f"--messages {num_records} "
        f"--bootstrap-server {BROKER_ADDRESS} "
        "--show-detailed-stats"
    )
    logger.log_all(f"Running consumer performance test with {num_records} records...")
    result = run_cmd(cmd, capture_output=True)
    return result.stdout

def parse_prod_perf_test(output):
    # For producer, take the last line with MB/sec in parentheses.
    lines = output.strip().splitlines()
    for line in reversed(lines):
        match = re.search(r'\((\d+\.\d+) MB/sec\)', line)
        if match:
            return float(match.group(1))
    return 0.0

def parse_consumer_perf_test(output):
    # For consumer, average MB.sec column, skipping the first row.
    mb_sec_values = []
    lines = output.strip().splitlines()
    for line in lines:
        if line.startswith('time,'):
            continue  # Skip header.
        fields = line.split(',')
        try:
            mb_sec = float(fields[3])  # MB.sec is the 4th column.
            mb_sec_values.append(mb_sec)
        except (IndexError, ValueError):
            continue
    if mb_sec_values:
        return sum(mb_sec_values) / len(mb_sec_values)
    else:
        return 0.0

def run_performance_tests():
    # Run producer and consumer tests concurrently and collect throughput.
    with concurrent.futures.ThreadPoolExecutor() as executor:
        producer_future = executor.submit(run_producer_test)
        consumer_future = executor.submit(run_consumer_test)
        
        producer_output = producer_future.result()
        consumer_output = consumer_future.result()

    # Parse throughput (MB/s) from performance test output.
    Tp = parse_prod_perf_test(producer_output)
    Tc = parse_consumer_perf_test(consumer_output)
    
    logger.log_all(f"Producer Throughput (Tp): {Tp} MB/s")
    logger.log_all(f"Consumer Throughput (Tc): {Tc} MB/s")

def main():
    setup_experiment_env(logger, CLUSTER_ID, num_brokers=5, num_partitions=3)    

    generate_payload(logger)
    run_performance_tests()

    cleanup(logger, CLUSTER_ID)
    logger.log_all("All done.")

if __name__ == "__main__":
    main()
