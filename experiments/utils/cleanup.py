import os

from experiments.utils.cluster_setup import docker_rm_all_with_label

def cleanup(logger, cluster_id):
    # Remove Docker containers and volumes associated with the cluster ID.
    docker_rm_all_with_label(logger, cluster_id)

    # Remove Docker Compose configuration, Jolokia wrapper script, and payload file if they exist.
    logger.log_all("Checking for existing Docker Compose configuration to remove...")
    if os.path.exists('experiments/docker-compose.yml'):
        os.remove('experiments/docker-compose.yml')
        logger.log_all("Docker Compose configuration removed.")
    logger.log_all("Checking for existing Jolokia wrapper script to remove...")
    if os.path.exists('experiments/jolokia-wrapper.sh'):
        os.remove('experiments/jolokia-wrapper.sh')
        logger.log_all("Jolokia wrapper script removed.")
    logger.log_all("Checking for existing payload file to remove...")
    if os.path.exists('experiments/payload.txt'):
        os.remove('experiments/payload.txt')
        logger.log_all("Payload file removed.")
