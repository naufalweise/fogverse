import time
from experiments.constants import BROKER_ADDRESS, NODE_PREFIX, TOPIC_NAME
from experiments.utils.generate_jolokia_wrapper import generate_jolokia_wrapper
from experiments.utils.run_cmd import run_cmd

def docker_rm_all_with_label(logger, cluster_id):
    # Remove all Docker containers and volumes with the specified cluster ID.
    # This is useful for cleaning up before starting a new experiment.
    # It uses the Docker CLI to filter containers and volumes by label.
    logger.log_all(f"Removing all Docker containers and volumes with cluster ID '{cluster_id}'...")
    run_cmd(f'docker ps -a --filter "label=cluster_id={cluster_id}" -q | xargs -r docker rm -f', capture_output=True)
    run_cmd(f'docker volume ls --filter "label=cluster_id={cluster_id}" -q | xargs -r docker volume rm', capture_output=True)
    logger.log_all(f"All Docker containers and volumes with cluster ID '{cluster_id}' removed.")

    # Remove images.
    result = run_cmd('docker images --format "{{.Repository}} {{.ID}}"', capture_output=True)
    for line in result.stdout.strip().split('\n'):
        if line.startswith(f'experiments-{NODE_PREFIX}'):
            repo, image_id = line.split()
            logger.log_all(f"Removing Docker image {repo} ({image_id})...")
            run_cmd(f'docker rmi {image_id}', capture_output=True)
            logger.log_all(f"Docker image {repo} ({image_id}) Removed.")

def generate_compose(logger, num_brokers=1):
    # Generate the Docker Compose configuration for the specified number of brokers.
    # This function calls a Python script to create the configuration file.
    
    logger.log_all("Generating Docker Compose configuration...")
    run_cmd(f'python -m experiments.utils.generate_docker_compose {num_brokers}', capture_output=True)
    logger.log_all(f"Docker Compose configuration generated with {num_brokers} broker(s).")

def docker_compose_up(logger):
    # Start the Kafka services using Docker Compose.
    logger.log_all("Starting Kafka services...")
    run_cmd('docker compose -f ./experiments/docker-compose.yml up -d', capture_output=True)
    logger.log_all("Kafka services started.")

def wait_labeled_containers(logger, cluster_id):
    # Wait for all containers with the given cluster_id label to be up.
    logger.log_all(f"Waiting for containers with cluster ID '{cluster_id}' to start...")
    while True:
        result = run_cmd(
            f'docker ps --filter "label=cluster_id={cluster_id}" --format "{{{{.Status}}}}"',
            capture_output=True
        )
        statuses = result.stdout.strip().splitlines()

        if statuses and all(s.startswith("Up") for s in statuses):
            logger.log_all(f"All containers with cluster ID '{cluster_id}' are up and running.")
            break

def create_topic(logger, num_partitions=1):
    # Create a Kafka topic with the specified number of partitions.
    # This is done using the Kafka CLI command to create a topic.
    logger.log_all(f"Creating Kafka topic '{TOPIC_NAME}'...")
    cmd = (
        "kafka/bin/kafka-topics.sh "
        "--create "
        f"--topic {TOPIC_NAME} "
        f"--partitions {num_partitions} "
        "--replication-factor 1 "
        f"--bootstrap-server {BROKER_ADDRESS}"
    )
    run_cmd(cmd, capture_output=True)
    logger.log_all(f"Kafka topic '{TOPIC_NAME}' created with {num_partitions} partition(s).")

def wait_for_topic_ready(logger):
    # Wait for the Kafka topic to be ready.
    # This is done by checking if the topic appears in the list of topics.
    # The function will keep checking until the topic is found.
    logger.log_all(f"Waiting for topic '{TOPIC_NAME}' to be ready...")
    while True:
        result = run_cmd(f'kafka/bin/kafka-topics.sh --list --bootstrap-server {BROKER_ADDRESS}', capture_output=True)
        topics = result.stdout.strip().splitlines()
        if TOPIC_NAME in topics:
            logger.log_all(f"Topic '{TOPIC_NAME}' is ready.")
            break
        time.sleep(2)

def setup_experiment_env(logger, cluster_id, num_brokers=1, num_partitions=1):
    # Set up the full experiment environment.
    docker_rm_all_with_label(logger, cluster_id)
    generate_compose(logger, num_brokers)
    generate_jolokia_wrapper()
    docker_compose_up(logger)
    wait_labeled_containers(logger, cluster_id)
    create_topic(logger, num_partitions)
    wait_for_topic_ready(logger)
