import subprocess
import time

from experiments.constants import CLUSTER_ID, CONTAINER_NAME, TOPIC_NAME

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
        result = run(f'docker ps --filter "name={container_name}-0" --format "{{{{.Status}}}}"', capture_output=True)
        status = result.stdout.strip()
        if status.startswith("Up"):
            break
        time.sleep(2)

def create_topic():
    cmd = (
        "kafka/bin/kafka-topics.sh "
        "--create "
        f"--topic {TOPIC_NAME} "
        "--partitions 1 "
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
    wait_for_container_up(CONTAINER_NAME)
    create_topic()
    wait_for_topic_ready()
    print("Kafka cluster is up and running.")

if __name__ == "__main__":
    main()
