import subprocess
import sys
import threading
import time

from concurrent.futures import ThreadPoolExecutor
from experiments.constants import CLUSTER_ID, FIRST_CONTAINER, TOPIC_NAME
from experiments.prod_throughput.clients import run_throughput_test
from experiments.prod_throughput.monitor import monitor_resource_usage, start_iostat_stream

def run(cmd, check=True, capture_output=False, **kwargs):
    return subprocess.run(cmd, shell=True, check=check, capture_output=capture_output, text=True, **kwargs)

def docker_rm_all_with_label(cluster_id):
    run(f'docker ps -a --filter "label=cluster_id={cluster_id}" -q | xargs -r docker rm -f')
    run(f'docker volume ls --filter "label=cluster_id={cluster_id}" -q | xargs -r docker volume rm')

def generate_compose():
    run('python -m experiments.generate_compose')

def docker_compose_up():
    run('docker compose -f ./experiments/docker-compose.yml up -d')

def wait_for_container_up(container_name):
    while True:
        result = run(f'docker ps --filter "name={container_name}" --format "{{{{.Status}}}}"', capture_output=True)
        status = result.stdout.strip()
        if status.startswith("Up"):
            break
        time.sleep(2)

def create_topic(num_partitions=1):
    cmd = (
        "kafka/bin/kafka-topics.sh "
        "--create "
        f"--topic {TOPIC_NAME} "
        f"--partitions {num_partitions} "
        "--replication-factor 1 "
        "--bootstrap-server localhost:9092"
    )
    run(cmd)

def wait_for_topic_ready():
    while True:
        result = run(f'kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092', capture_output=True)
        topics = result.stdout.strip().splitlines()
        if TOPIC_NAME in topics:
            break
        time.sleep(2)

def main():
    docker_rm_all_with_label(CLUSTER_ID)
    generate_compose()
    docker_compose_up()
    wait_for_container_up(FIRST_CONTAINER)
    create_topic()
    wait_for_topic_ready()
    print("Kafka cluster is up and running.")

    if not start_iostat_stream():
        sys.exit("Failed to initialize iostat monitoring. Exiting.")
    else:
        time.sleep(4)

        # Start resource monitoring in a separate thread (no return value).
        monitor_thread = threading.Thread(target=monitor_resource_usage)
        monitor_thread.start()

        # Start throughput test in a thread and capture return value.
        with ThreadPoolExecutor() as executor:
            future = executor.submit(run_throughput_test)

        # Wait for both to finish.
        monitor_thread.join()
        tp, tc = future.result()

        print(f"Production Throughput per Partition: {tp} MiB/s")
        print(f"Consumption Throughput per Partition: {tc} MiB/s")

if __name__ == "__main__":
    main()
