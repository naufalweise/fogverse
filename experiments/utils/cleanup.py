import os

from experiments.constants import CLUSTER_ID
from experiments.utils.cluster_setup import docker_rm_all_with_label

def cleanup(logger, cluster_id=CLUSTER_ID):
    # Remove Docker containers and volumes associated with the cluster ID.
    docker_rm_all_with_label(logger, cluster_id)

    # Remove Docker Compose configuration, Jolokia wrapper script, and payload file if they exist.
    logger.log_all("Checking for existing Docker Compose configuration to remove...")
    if os.path.exists('docker-compose.yml'):
        os.remove('docker-compose.yml')
        logger.log_all("Docker Compose configuration removed.")
    logger.log_all("Checking for existing Jolokia wrapper script to remove...")
    if os.path.exists('jolokia-wrapper.sh'):
        os.remove('jolokia-wrapper.sh')
        logger.log_all("Jolokia wrapper script removed.")
    logger.log_all("Checking for existing payload file to remove...")
    if os.path.exists('payload.txt'):
        os.remove('payload.txt')
        logger.log_all("Payload file removed.")

    # Remove any other temporary files or directories created during the experiment.
    for file in os.listdir("."):
        if file.endswith("logs.txt"):
            os.remove(file)
