import concurrent.futures
import os
import re
import subprocess
import sys
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
    num_instances=1,
    min_heap=KAFKA_HEAP_MIN,
    max_heap=KAFKA_HEAP_MAX,
    log_output=False,
    track_progress=True
):
    # Spawn multiple instances of producer test based on num_instances.
    logger.log_all(f"Running {num_instances} producer instance(s) with total {num_records} records...")

    per_instance_records = num_records // num_instances
    per_instance_throughput = throughput // num_instances if throughput > 0 else -1

    processes = []
    env = os.environ.copy()
    env["KAFKA_HEAP_OPTS"] = f"{min_heap} {max_heap}"

    for i in range(num_instances):
        cmd = (
            "kafka/bin/kafka-producer-perf-test.sh "
            f"--producer-props acks=all bootstrap.servers={bootstrap_server} "
            f"--topic {topic_name} "
            f"--num-records {per_instance_records} "
            f"{'--payload-file payload.txt' if record_size is None else f'--record-size {record_size}'} "
            f"--throughput {per_instance_throughput} "
        )
        logger.log_all(f"Launching producer instance {i + 1}...")
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        processes.append((i, process))

    total_output = []
    for i, process in processes:
        output = []
        running_total = 0
        for line in process.stdout:
            line = line.strip()
            formatted_line = f"Producer {i + 1}: {line}"

            if log_output: logger.log_all(formatted_line)
            output.append(formatted_line)

            if track_progress:
                match = re.match(r"(\d+)\s+records sent", line)
                if match:
                    this_count = int(match.group(1))
                    running_total += this_count
                    percent = int((running_total / per_instance_records) * 100)
                    percent = min(percent, 100)
                    logger.log_all(f"Producer {i + 1}: {percent}% progress completed.")

        process.wait()
        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, cmd)

        total_output.extend(output)

    return "\n".join(total_output)

def run_consumer_test(
    logger=logger,
    bootstrap_server=BOOTSTRAP_SERVER,
    topic_name=TOPIC_NAME,
    num_records=NUM_RECORDS,
    num_instances=1,
    min_heap=KAFKA_HEAP_MIN,
    max_heap=KAFKA_HEAP_MAX,
    log_output=False
):
    # Spawn multiple instances of consumer test based on num_instances.
    logger.log_all(f"Running {num_instances} consumer instance(s) with total {num_records} records...")

    per_instance_records = num_records // num_instances
    processes = []
    env = os.environ.copy()
    env["KAFKA_HEAP_OPTS"] = f"{min_heap} {max_heap}"

    for i in range(num_instances):
        cmd = (
            "kafka/bin/kafka-consumer-perf-test.sh "
            f"--bootstrap-server {bootstrap_server} "
            f"--topic {topic_name} "
            f"--messages {per_instance_records} "
            "--timeout 72000 "
            "--group test-group"
        )
        logger.log_all(f"Launching consumer instance {i + 1}...")
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        processes.append((i, process))

    total_output = []
    for i, process in processes:
        output = []
        for line in process.stdout:
            line = line.strip()
            formatted_line = f"Consumer {i + 1}: {line}"

            if log_output: logger.log_all(formatted_line)
            output.append(formatted_line)

        process.wait()
        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, cmd)

        total_output.extend(output)

    return "\n".join(total_output)

def parse_prod_perf_test(output, logger=logger):
    # Holds per-instance (throughput MB/sec, average latency ms) tuples.
    instance_summaries = {}

    for line in output.strip().splitlines():
        # Match lines containing per-producer throughput and latency stats.
        if "Producer" in line and "th" in line:  # "th" hints at presence of a summary line.
            match = re.match(
                r"Producer (\d+): .*?(\d+\.\d+)\s+records/sec\s+\((\d+\.\d+)\s+MB/sec\).*?(\d+\.\d+)\s+ms avg latency",
                line
            )
            if match:
                instance_id = int(match.group(1))
                throughput = float(match.group(3))  # The index for MB/sec.
                latency = float(match.group(4))  # The index for avg latency.

                # Only keep the first valid stat line per producer instance.
                if instance_id not in instance_summaries:
                    instance_summaries[instance_id] = (throughput, latency)
            else:
                # Log unparseable lines for visibility/debugging.
                logger.log_all(f"No match found in line:\n{line}")

    logger.log_all(f"Parsed producer performance data (throughput, latency): {instance_summaries}")

    # Aggregate total throughput and average latency across all producer instances.
    total_throughput = sum(t for t, _ in instance_summaries.values())

    latencies = [l for _, l in instance_summaries.values()]
    avg_latency = sum(latencies) / len(latencies) if latencies else sys.maxsize

    return total_throughput, avg_latency


def parse_consumer_perf_test(output, logger=logger):
    # Holds per-instance (throughput MB/sec, fetch time ms) tuples.
    instance_results = {}

    # Match start of a consumer stat line.
    pattern = re.compile(r"Consumer\s+(\d+):\s+(.*)")

    for line in output.strip().splitlines():
        match = pattern.match(line)
        if not match:
            continue

        instance_id = int(match.group(1))
        data_line = match.group(2)

        # Skip header line which often starts with "start.time".
        if data_line.startswith("start.time"):
            continue

        fields = data_line.split(',')
        if len(fields) < 8:
            continue  # Skip malformed lines with missing fields.

        try:
            mb_sec = float(fields[3])  # The index for MB/sec.
            fetch_time = float(fields[7])  # The index for fetch time.

            # Only keep the first valid stat per consumer instance.
            if instance_id not in instance_results:
                instance_results[instance_id] = (mb_sec, fetch_time)
        except ValueError:
            continue  # Skip if parsing any field fails.

    logger.log_all(f"Parsed consumer performance data (throughput, fetch time): {instance_results}")

    # Aggregate total throughput and average fetch time across all consumer instances.
    total_throughput = sum(t for t, _ in instance_results.values())

    fetch_latencies = [l for _, l in instance_results.values()]
    avg_fetch_time = sum(fetch_latencies) / len(fetch_latencies) if fetch_latencies else sys.maxsize

    return total_throughput, avg_fetch_time

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
