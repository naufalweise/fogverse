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

    # Remove images.
    result = run_cmd('docker images --format "{{.Repository}} {{.ID}}"', capture_output=True)
    for line in result.stdout.strip().split('\n'):
        if line.startswith(f'experiments-{NODE_PREFIX}'):
            repo, image_id = line.split()
            logger.log_all(f"Removing Docker image {repo} ({image_id})...")
            run_cmd(f'docker rmi {image_id}', capture_output=True)

    logger.log_all(f"Removal of Docker containers and volumes for cluster ID '{cluster_id}' completed.")

def generate_compose(logger, num_brokers=1):
    # Generate the Docker Compose configuration for the specified number of brokers.
    # This function calls a Python script to create the configuration file.
    
    logger.log_all("Generating Docker Compose configuration...")
    run_cmd(f'python -m experiments.utils.generate_docker_compose {num_brokers}', capture_output=True)
    logger.log_all(f"Docker Compose configuration generated successfully with {num_brokers} broker(s).")

def docker_compose_up(logger):
    # Start the Kafka services using Docker Compose.
    logger.log_all("Starting Kafka services...")
    run_cmd('docker compose -f ./experiments/docker-compose.yml up -d', capture_output=True)
    logger.log_all("Kafka services started.")

def wait_for_container_up(logger, container_names):
    # Wait for the specified Docker containers to be up and running.
    # This is done by checking the status of each container.
    # The function will keep checking until the container is up.
    for container_name in container_names:
        logger.log_all(f"Waiting for container {container_name} to start...")
        while True:
            result = run_cmd(f'docker ps --filter "name={container_name}" --format "{{{{.Status}}}}"', capture_output=True)
            status = result.stdout.strip()
            if status.startswith("Up"):
                logger.log_all(f"Container {container_name} is up and running.")
                break
            time.sleep(2)

def create_topic(logger, num_partitions=1):
    # Create a Kafka topic with the specified number of partitions.
    # This is done using the Kafka CLI command to create a topic.
    logger.log_all(f"Creating Kafka topic '{TOPIC_NAME}' with {num_partitions} partition(s)...")
    cmd = (
        "kafka/bin/kafka-topics.sh "
        "--create "
        f"--topic {TOPIC_NAME} "
        f"--partitions {num_partitions} "
        "--replication-factor 1 "
        f"--bootstrap-server {BROKER_ADDRESS}"
    )
    run_cmd(cmd, capture_output=True)
    logger.log_all(f"Kafka topic '{TOPIC_NAME}' created successfully.")

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

def setup_experiment_env(logger, cluster_id, container_names, num_brokers=1, num_partitions=1, enable_jolokia=True):
    # Set up the full experiment environment.
    docker_rm_all_with_label(logger, cluster_id)
    generate_compose(logger, num_brokers)
    if enable_jolokia: generate_jolokia_wrapper()
    docker_compose_up(logger)
    wait_for_container_up(logger, container_names)
    create_topic(logger, num_partitions)
    wait_for_topic_ready(logger)
