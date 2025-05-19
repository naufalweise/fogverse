import concurrent.futures
import os
import re
import subprocess
import time

from experiments.constants import BOOTSTRAP_SERVER, KAFKA_HEAP_MAX, KAFKA_HEAP_MIN, NUM_RECORDS, TOPIC_NAME
from experiments.utils.cleanup import cleanup
from experiments.utils.cluster_setup import setup_experiment_env
from experiments.utils.generate_payload import generate_payload
from fogverse.logger.fog import FogLogger

logger = FogLogger(f"throughput_{int(time.time())}")

def run_producer_test(
    logger=logger,
    bootstrap_server=BOOTSTRAP_SERVER,
    topic_name=TOPIC_NAME,
    num_records=NUM_RECORDS,
    record_size=None,
    throughput=-1,
    log_output=False,
    track_progress=True
):
    # Run the Kafka producer performance test using payload.txt and track progress.
    cmd = (
        "kafka/bin/kafka-producer-perf-test.sh "
        f"--producer-props acks=all bootstrap.servers={bootstrap_server} "
        f"--topic {topic_name} "
        f"--num-records {num_records} "
        f"{'--payload-file payload.txt' if record_size is None else f'--record-size {record_size}'} "
        f"--throughput {throughput} "
    )
    logger.log_all(f"Running producer with {num_records} records...")

    env = os.environ.copy()
    env["KAFKA_HEAP_OPTS"] = f"{KAFKA_HEAP_MIN} {KAFKA_HEAP_MAX}"  # Adjust heap size as needed.
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)
    output = []

    running_total = 0
    for line in process.stdout:
        line = line.strip()
        if log_output: 
            logger.log_all(line)

        output.append(line)

        if track_progress:
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

def run_consumer_test(logger, bootstrap_server=BOOTSTRAP_SERVER, topic_name=TOPIC_NAME, num_records=NUM_RECORDS, log_output=False):
    # Run the Kafka consumer performance test.
    cmd = (
        "kafka/bin/kafka-consumer-perf-test.sh "
        f"--bootstrap-server {bootstrap_server} "
        f"--topic {topic_name} "
        f"--messages {num_records} "
        "--timeout 72000 "
        "--show-detailed-stats"
    )
    logger.log_all(f"Running consumer with {num_records} records...")

    env = os.environ.copy()
    env["KAFKA_HEAP_OPTS"] = "-Xms2g -Xmx16g"  # Adjust heap size as needed.
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)
    output = []

    for line in process.stdout:
        line = line.strip()
        if log_output:
            logger.log_all(line)

        output.append(line)

    process.wait()
    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, cmd)

    return "\n".join(output)

def parse_prod_perf_test(output):
    lines = output.strip().splitlines()
    for line in reversed(lines):
        throughput_match = re.search(r'(\d+\.\d+)\s+MB/sec', line)
        latency_match = re.search(r'(\d+\.\d+)\s+ms avg latency', line)
        if throughput_match and latency_match:
            mb_per_sec = float(throughput_match.group(1))
            avg_latency_ms = float(latency_match.group(1))
            return mb_per_sec, avg_latency_ms
    return 0.0, 0.0

def parse_consumer_perf_test(output):
    mb_sec_vals = []
    fetch_times = []

    for line in output.strip().splitlines():
        if line.startswith('time,'):
            continue  # Skip header.

        fields = line.split(',')
        try:
            mb_sec_vals.append(float(fields[3]))  # MB.sec is the 4th column.
            fetch_times.append(float(fields[7]))  # fetch time (ms) is the 8th column.
        except (IndexError, ValueError):
            continue  # skip bad lines.

    # Compute averages (0.0 if no data).
    avg_mb = sum(mb_sec_vals) / len(mb_sec_vals) if mb_sec_vals else 0.0
    avg_fetch = sum(fetch_times) / len(fetch_times) if fetch_times else 0.0

    return avg_mb, avg_fetch

def run_performance_tests():
    # Run producer and consumer tests concurrently and collect throughput.
    with concurrent.futures.ThreadPoolExecutor() as executor:
        producer_future = executor.submit(run_producer_test)
        consumer_future = executor.submit(run_consumer_test)
        
        producer_output = producer_future.result()
        consumer_output = consumer_future.result()

    # Parse throughput (MB/s) from performance test output.
    producer_throughput = parse_prod_perf_test(producer_output)[0] * 1_000_000  # Convert from MB/s to bytes/s.
    consumer_throughput = parse_consumer_perf_test(consumer_output)[0] * 1_000_000  # Convert from MB/s to bytes/s.

    logger.log_all(f"Production Throughput: {producer_throughput} bytes/s")
    logger.log_all(f"Consumption Throughput: {consumer_throughput} bytes/s")

    return producer_throughput, consumer_throughput

def main():
    logger.log_all("Throughput measurement initiated.")
    setup_experiment_env(logger)

    generate_payload(logger)
    producer_throughput, consumer_throughput = run_performance_tests()

    cleanup(logger)
    logger.log_all("Throughput measurement completed.")

    return producer_throughput, consumer_throughput

if __name__ == "__main__":
    main()
