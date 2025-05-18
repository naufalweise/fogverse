import requests
import time

from experiments.constants import BASE_PORT, MBEAN_PRODUCE_REMOTETIME
from experiments.throughput import run_producer_test
from experiments.utils.cleanup import cleanup
from experiments.utils.cluster_setup import setup_experiment_env
from experiments.utils.generate_payload import generate_payload
from fogverse.logger.fog import FogLogger

logger = FogLogger(f"replication_latency_{int(time.time())}")

def get_jolokia_metrics_urls(num_brokers, base_port=BASE_PORT):
    # Generate a list of Jolokia urls for the given number of brokers.
    # Each broker's Jolokia endpoint is assumed to be on the same host with a specific port offset.
    urls = []
    for i in range(num_brokers):
        jolokia_host_port = base_port + (10 * i) + 6
        urls.append(f"http://localhost:{jolokia_host_port}/jolokia")
    return urls

def get_jmx_metric(url, mbean):
    # Fetch a JMX metric from the Jolokia endpoint.
    # The mbean is the specific metric to retrieve.
    response = requests.get(f"{url}/read/{mbean}", timeout=4)
    data = response.json()
    return data.get("value")

def measure_replication_latency(num_brokers, base_port=BASE_PORT, mbean=MBEAN_PRODUCE_REMOTETIME):
    """
    Measure and log the average replication latency from Jolokia metrics across Kafka brokers.
    Only considers brokers that act as leader partitions (indicated by Count > 0 or OneMinuteRate > 0).
    """
    urls = get_jolokia_metrics_urls(num_brokers, base_port)
    valid_latencies = []

    for url in urls:
        metrics = get_jmx_metric(url, mbean)
        if not metrics:
            continue

        count = metrics.get("Count", 0)
        one_min_rate = metrics.get("OneMinuteRate", 0.0)

        if count > 0 or one_min_rate > 0.0:
            latency = metrics.get("Mean") / 1_000  # Convert from milliseconds to seconds.
            if latency is not None:
                valid_latencies.append(latency)
                logger.log_all(f"Replication latency from {url}: {latency:.2f} second(s)")
            else:
                logger.log_all(f"Replication latency from {url}: N/A")

    if valid_latencies:
        avg_latency = sum(valid_latencies) / len(valid_latencies)
        logger.log_all(f"Average replication latency: {avg_latency:.2f} second(s)")
        return avg_latency
    else:
        logger.log_all("No valid replication latency metrics found.")
        return None

def main():
    logger.log_all("Replication latency measurement initiated.")

    num_brokers = 3
    replication_factor = 3

    setup_experiment_env(logger, num_brokers=num_brokers, replication_factor=replication_factor)

    generate_payload(logger)
    run_producer_test(logger)
    avg_latency = measure_replication_latency(num_brokers)

    cleanup(logger)
    logger.log_all("Replication latency measurement completed.")

    return avg_latency

if __name__ == "__main__":
    main()
