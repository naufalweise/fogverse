import concurrent.futures
import re
import subprocess
import time

from experiments.constants import BOOTSTRAP_SERVER, NUM_RECORDS, TOPIC_NAME
from experiments.utils.cleanup import cleanup
from experiments.utils.cluster_setup import setup_experiment_env
from experiments.utils.generate_payload import generate_payload
from experiments.utils.run_cmd import run_cmd
from fogverse.logger.fog import FogLogger

logger = FogLogger(f"throughput_{int(time.time())}")

def run_producer_test(logger=logger, bootstrap_server=BOOTSTRAP_SERVER, topic_name=TOPIC_NAME, num_records=NUM_RECORDS//16):
    # Run the Kafka producer performance test using payload.txt and track progress.
    cmd = (
        "kafka/bin/kafka-producer-perf-test.sh "
        f"--producer-props acks=all bootstrap.servers={bootstrap_server} "
        f"--topic {topic_name} "
        f"--num-records {num_records} "
        "--payload-file payload.txt "
        "--throughput -1 "
    )
    logger.log_all(f"Running producer with {num_records} records...")

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

def run_consumer_test(bootstrap_server=BOOTSTRAP_SERVER, topic_name=TOPIC_NAME, num_records=NUM_RECORDS//16):
    # Run the Kafka consumer performance test.
    cmd = (
        "kafka/bin/kafka-consumer-perf-test.sh "
        f"--bootstrap-server {bootstrap_server} "
        f"--topic {topic_name} "
        f"--messages {num_records} "
        "--show-detailed-stats"
    )
    logger.log_all(f"Running consumer with {num_records} records...")
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
    producer_throughput = parse_prod_perf_test(producer_output) * 1_000_000  # Convert from MB/s to bytes/s.
    consumer_throughput = parse_consumer_perf_test(consumer_output) * 1_000_000  # Convert from MB/s to bytes/s.

    logger.log_all(f"Producer Throughput: {producer_throughput} bytes/s")
    logger.log_all(f"Consumer Throughput: {consumer_throughput} bytes/s")

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
