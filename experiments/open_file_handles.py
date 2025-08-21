import re
import subprocess
import time

from experiments.constants import CONTAINER_PREFIX
from experiments.utils.cleanup import cleanup
from experiments.utils.cluster_setup import setup_experiment_env
from fogverse.logger.fog import FogLogger

logger = FogLogger(f"open_file_handles_{int(time.time())}")

def get_open_file_limit_from_container(container_prefix=CONTAINER_PREFIX):
    open_files_limit = None

    try:
        # Get container IDs whose names start with container_prefix.
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.ID}} {{.Names}}"],
            capture_output=True, text=True, check=True
        )
        containers = result.stdout.strip().splitlines()

        target_container = None
        for line in containers:
            container_id, name = line.strip().split(maxsplit=1)
            if name.startswith(container_prefix):
                target_container = container_id
                break

        if not target_container:
            logger.log_all(f"No container found with prefix '{container_prefix}'.")
            return

        # Run `ulimit -a` inside the container.
        ulimit_output = subprocess.run(
            ["docker", "exec", target_container, "ulimit", "-a"],
            capture_output=True, text=True, check=True
        ).stdout

        # Extract "open files" value using regex.
        match = re.search(r"open files\s+\(\-n\)\s+(\d+)", ulimit_output)
        if match:
            open_files_limit = match.group(1)
            logger.log_all(f"Open file handle limit: {open_files_limit} files")
        else:
            logger.log_all("Could not parse 'open files' limit from ulimit output.")

    except subprocess.CalledProcessError as e:
        logger.log_all(f"DOCKER COMMAND ERROR: {e}")
    except Exception as e:
        logger.log_all(f"UNKNOWN ERROR: {e}")

    return open_files_limit

def main():
    logger.log_all("Open file handles evaluation initiated.")
    setup_experiment_env(logger)    

    open_files_limit = get_open_file_limit_from_container()

    cleanup(logger)
    logger.log_all("Open file handles evaluation completed.")

    return open_files_limit

if __name__ == "__main__":
    main()
