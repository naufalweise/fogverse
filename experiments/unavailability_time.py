import time
import re

from datetime import datetime
from experiments.constants import CONTAINER_PREFIX
from experiments.utils.cleanup import cleanup
from experiments.utils.cluster_setup import setup_experiment_env
from experiments.utils.run_cmd import run_cmd
from fogverse.logger.fog import FogLogger

logger = FogLogger(f"unavailability_time_{int(time.time())}")

def get_container_names(num_brokers, container_prefix=CONTAINER_PREFIX):
    # Returns a list of container names for broker/controller containers.
    # Assumes containers are named with even-numbered suffixes, starting from 0.
    return [f"{container_prefix}-{i * 2}" for i in range(num_brokers)]

def save_logs(container_name):
    # Save logs from the specified container to a file.
    # The log file is named after the container, with a .txt extension.
    log_path = f"{container_name}-logs.txt"
    run_cmd(f"docker logs {container_name} > {log_path}")
    return log_path

def wait_logmatch(container_name, pattern):
    # Waits for a specific log line pattern to appear in the logs of a given container.
    # Continuously saves fresh logs until the pattern is found.
    regex = re.compile(pattern)
    log_path = f"{container_name}-logs.txt"

    while True:
        save_logs(container_name)

        try:
            with open(log_path) as f:
                for line in f:
                    if regex.search(line):
                        return line
        except FileNotFoundError:
            pass  # Silently skip if file doesn't exist.

        time.sleep(0.5)

def extract_timestamp(log_line):
    # Extract timestamp from log line e.g. [1970-01-01 00:00:00,000].
    match = re.match(r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\]", log_line)
    if match:
        timestamp_str = match.group(1)
        return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S,%f")
    return None

def measure_unavailability_time(num_brokers, kill_count):
    container_names = get_container_names(num_brokers)
    unavailability_times = []

    for _ in range(kill_count):
        stopped_broker = container_names.pop(0)

        logger.log_all(f"Stopping {stopped_broker}...")

        run_cmd(f"docker stop {stopped_broker}")

        shutdown_line = wait_logmatch(
            stopped_broker,
            r"Transition from STARTED to SHUTTING_DOWN"
        )
        shutdown_time = extract_timestamp(shutdown_line)

        logger.log_all(f"Shutdown recorded for {stopped_broker} at {shutdown_time}.")

        min_fence_time = None
        stopped_id = int(stopped_broker.split("-")[-1])

        for running_broker in container_names:
            fence_line = wait_logmatch(
                running_broker,
                rf"Replayed BrokerRegistrationChangeRecord.*broker {stopped_id}.*fenced=1"
            )
            fence_time = extract_timestamp(fence_line)

            logger.log_all(f"Recorded fence for {stopped_broker} by {running_broker} at {fence_time}.")

            if min_fence_time is None or fence_time < min_fence_time:
                min_fence_time = fence_time

        if min_fence_time and shutdown_time:
            unavailability_ms = int((min_fence_time - shutdown_time).total_seconds() * 1000)
            logger.log_all(f"Unavailability time for {stopped_broker}: {unavailability_ms} ms")
            unavailability_times.append(unavailability_ms)

    if unavailability_times:
        avg_time = sum(unavailability_times) / len(unavailability_times)
        logger.log_all(f"Average unavailability time: {avg_time:.2f} ms")
        return avg_time
    else:
        logger.log_all("No unavailability times recorded.")
        return None

def main():
    logger.log_all("Unavailability time measurement initiated.")

    num_brokers = 3
    kill_count = 1

    if kill_count >= num_brokers:
        logger.log_all("Kill count must be less than the number of brokers.")
        return

    setup_experiment_env(logger, num_brokers=num_brokers)

    measure_unavailability_time(num_brokers, kill_count)

    cleanup(logger)
    logger.log_all("Unavailability time measurement completed.")

if __name__ == "__main__":
    main()
