import os

from experiments.utils.cluster_setup import docker_rm_all_with_label

def cleanup(logger, cluster_id):
    logger.log_all("Cleaning up...")

    # Remove Docker containers and volumes associated with the cluster ID.
    docker_rm_all_with_label(logger, cluster_id)

    # Remove Docker Compose configuration and Jolokia wrapper script if they exist.
    if os.path.exists('experiments/docker-compose.yml'):
        os.remove('experiments/docker-compose.yml')
    if os.path.exists('experiments/jolokia-wrapper.sh'):
        os.remove('experiments/jolokia-wrapper.sh')
    if os.path.exists('experiments/payload.txt'):
        os.remove('experiments/payload.txt')

    logger.log_all("Cleanup completed.")
