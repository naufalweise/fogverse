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
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)
        processes.append((i, process))

    total_output = []
    for i, process in processes:
        output = []
        running_total = 0
        for line in process.stdout:
            line = line.strip()
            if log_output:
                logger.log_all(f"Producer {i + 1}: {line}")
            output.append(line)

            if track_progress:
                match = re.match(r"(\d+)\s+records sent", line)
                if match:
                    this_count = int(match.group(1))
                    running_total += this_count
                    percent = int((running_total / per_instance_records) * 100)
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
            "--group test-group "
            "--show-detailed-stats"
        )
        logger.log_all(f"Launching consumer instance {i + 1}...")
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)
        processes.append((i, process))

    total_output = []
    for i, process in processes:
        output = []
        for line in process.stdout:
            line = line.strip()
            if log_output:
                logger.log_all(f"Consumer {i + 1}: {line}")
            output.append(line)

        process.wait()
        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, cmd)

        total_output.extend(output)

    return "\n".join(total_output)

def parse_prod_perf_test(output):
    instance_data = {}  # {instance_id: [(throughput, latency), ...]}.

    for line in output.strip().splitlines():
        match = re.match(r"Producer (\d+): .*?(\d+\.\d+)\s+MB/sec.*?(\d+\.\d+)\s+ms avg latency", line)
        if match:
            instance = int(match.group(1))
            throughput = float(match.group(2))
            latency = float(match.group(3))
            instance_data.setdefault(instance, []).append((throughput, latency))

    avg_throughputs = []
    avg_latencies = []

    for records in instance_data.values():
        if records:
            avg_throughput = sum(t for t, _ in records) / len(records)
            avg_latency = sum(l for _, l in records) / len(records)
            avg_throughputs.append(avg_throughput)
            avg_latencies.append(avg_latency)

    total_avg_throughput = sum(avg_throughputs) if avg_throughputs else 0.0
    total_avg_latency = sum(avg_latencies) if avg_latencies else 0.0

    return total_avg_throughput, total_avg_latency

def parse_consumer_perf_test(output):
    instance_data = {}  # {instance_id: [(mb_sec, fetch_time), ...]}

    for line in output.strip().splitlines():
        if not line.startswith("Consumer"):
            continue

        match = re.match(r"Consumer (\d+): (.*)", line)
        if not match:
            continue

        instance_id = int(match.group(1))
        data_line = match.group(2)

        if data_line.startswith("start.time"):
            continue  # Skip headers

        fields = data_line.split(',')
        try:
            mb_sec = float(fields[3])  # MB/sec.
            fetch_time = float(fields[7])  # fetch time in ms.
            instance_data.setdefault(instance_id, []).append((mb_sec, fetch_time))
        except (IndexError, ValueError):
            continue

    avg_throughputs = []
    avg_fetch_latencies = []

    for records in instance_data.values():
        if records:
            avg_throughput = sum(t for t, _ in records) / len(records)
            avg_fetch = sum(l for _, l in records) / len(records)
            avg_throughputs.append(avg_throughput)
            avg_fetch_latencies.append(avg_fetch)

    total_avg_throughput = sum(avg_throughputs) if avg_throughputs else 0.0
    total_avg_fetch = sum(avg_fetch_latencies) if avg_fetch_latencies else 0.0

    return total_avg_throughput, total_avg_fetch

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
