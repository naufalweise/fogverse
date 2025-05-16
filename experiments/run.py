import subprocess
import time
import os
import concurrent.futures
import re
from experiments.constants import BROKER_ADDRESS, CLUSTER_ID, FIRST_CONTAINER, NUM_RECORDS, TOPIC_NAME
from fogverse.logger.fog import FogLogger

logger = FogLogger(f"throughput_{int(time.time())}")

def run(cmd, check=True, capture_output=False, **kwargs):
    try:
        return subprocess.run(cmd, shell=True, check=check, capture_output=capture_output, text=True, **kwargs)
    except subprocess.CalledProcessError as e:
        if capture_output:
            print(f"Command failed: {cmd}")
            print(f"STDERR: {e.stderr}")
            print(f"STDOUT: {e.stdout}")
        raise

def docker_rm_all_with_label(cluster_id):
    # Remove all Docker containers and volumes with the specified cluster ID.
    run(f'docker ps -a --filter "label=cluster_id={cluster_id}" -q | xargs -r docker rm -f', capture_output=True)
    run(f'docker volume ls --filter "label=cluster_id={cluster_id}" -q | xargs -r docker volume rm', capture_output=True)

def generate_compose():
    # Generate the Docker Compose configuration file.
    logger.log_all("Generating Docker Compose configuration...")
    run('python -m experiments.generate_compose', capture_output=True)
    logger.log_all("Docker Compose configuration generated successfully.")

def docker_compose_up():
    # Start the Docker Compose services.
    logger.log_all("Starting Kafka services...")
    run('docker compose -f ./experiments/docker-compose.yml up -d', capture_output=True)
    logger.log_all("Kafka services started.")

def wait_for_container_up(container_name):
    # Wait until the specified container is running.
    logger.log_all(f"Waiting for container {container_name} to start...")
    while True:
        result = run(f'docker ps --filter "name={container_name}" --format "{{{{.Status}}}}"', capture_output=True)
        status = result.stdout.strip()
        if status.startswith("Up"):
            logger.log_all(f"Container {container_name} is up and running.")
            break
        time.sleep(2)

def create_topic(num_partitions=1):
    # Create a Kafka topic with the specified number of partitions.
    logger.log_all(f"Creating Kafka topic '{TOPIC_NAME}' with {num_partitions} partitions...")
    cmd = (
        "kafka/bin/kafka-topics.sh "
        "--create "
        f"--topic {TOPIC_NAME} "
        f"--partitions {num_partitions} "
        "--replication-factor 1 "
        f"--bootstrap-server {BROKER_ADDRESS}"
    )
    run(cmd, capture_output=True)
    logger.log_all(f"Kafka topic '{TOPIC_NAME}' created successfully.")

def wait_for_topic_ready():
    # Wait until the Kafka topic is ready.
    logger.log_all(f"Waiting for topic '{TOPIC_NAME}' to be ready...")
    while True:
        result = run(f'kafka/bin/kafka-topics.sh --list --bootstrap-server {BROKER_ADDRESS}', capture_output=True)
        topics = result.stdout.strip().splitlines()
        if TOPIC_NAME in topics:
            logger.log_all(f"Topic '{TOPIC_NAME}' is ready.")
            break
        time.sleep(2)

def generate_payload_file(filepath='payload.txt',min_kb=1,max_kb=100,step_kb=1,char='*'):
    logger.log_all(f"Generating payload file from {min_kb}KB to {max_kb}KB in steps of {step_kb}KB...")
    with open(filepath, 'w') as f:
        for kb in range(min_kb, max_kb + 1, step_kb):
            payload = char * (kb * 1024)
            f.write(payload + '\n')  # Each payload on a new line.
    logger.log_all(f"Generated {filepath} successfully.")

def run_producer_test(num_records=NUM_RECORDS):
    # Run the Kafka producer performance test using payload.txt and track progress.
    cmd = (
        "kafka/bin/kafka-producer-perf-test.sh "
        f"--topic {TOPIC_NAME} "
        f"--num-records {num_records} "
        "--payload-file payload.txt "
        "--throughput -1 "
        f"--producer-props bootstrap.servers={BROKER_ADDRESS}"
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
    result = run(cmd, capture_output=True)
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
    # Set up and start the Kafka cluster, then run performance tests.
    docker_rm_all_with_label(CLUSTER_ID)
    generate_compose()
    docker_compose_up()
    wait_for_container_up(FIRST_CONTAINER)
    create_topic()
    wait_for_topic_ready()
    
    # Generate payload and run performance tests.
    generate_payload_file()
    run_performance_tests()
    
    logger.log_all("Cleaning up...")
    docker_rm_all_with_label(CLUSTER_ID)
    if os.path.exists('payload.txt'):
        os.remove('payload.txt')
    logger.log_all("Cleanup completed.")

if __name__ == "__main__":
    main()
