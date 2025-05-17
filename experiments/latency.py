import time

from experiments.constants import CLUSTER_ID
from experiments.throughput import run_producer_test
from experiments.utils.cleanup import cleanup
from experiments.utils.cluster_setup import setup_experiment_env
from experiments.utils.generate_payload import generate_payload
from fogverse.logger.fog import FogLogger
import requests # pip install requests

logger = FogLogger(f"throughput_{int(time.time())}")

MBEAN_PRODUCE_REMOTETIME = "kafka.network:type=RequestMetrics,name= ,request=Produce"

def get_jolokia_urls(num_brokers, base_kafka_port=9090):
    urls = []
    for i in range(num_brokers):
        node_id = i * 2
        jolokia_host_port = base_kafka_port + (10 * i) + 6
        urls.append(f"http://localhost:{jolokia_host_port}/jolokia")
    return urls

def get_jmx_metric(jolokia_url, mbean, attribute=None):
    response = requests.get(f"{jolokia_url}/read/{mbean}", timeout=5)
    data = response.json()
    return data.get("value")

def measure_replication_latency():    

    leader_metrics_found = False
    print("\n--- Replication Latency (RemoteTimeMs for Produce Requests) ---")
    for i, jolokia_url in enumerate(jolokia_urls):
        metrics = get_jmx_metric(jolokia_url, MBEAN_PRODUCE_REMOTETIME)
        
        if metrics:
            count = metrics.get("Count", 0)
            one_min_rate = metrics.get("OneMinuteRate", 0.0)

            # Heuristic: if this broker processed produce requests recently, its Count will be higher
            # or OneMinuteRate will be non-zero.
            # For a fresh test, the leader of the single partition topic should have a count >= num_messages
            if count > 0 or one_min_rate > 0.01 : # Check if there's substantial activity
                leader_metrics_found = True
                print(f"  Latency Statistics (ms):")
                print(f"    Mean  : {metrics.get('Mean', 'N/A'):.3f}")

def main():
    setup_experiment_env(logger, CLUSTER_ID, num_brokers = 5, num_partitions = 3)

    generate_payload(logger)
    run_producer_test()
    measure_replication_latency()

    cleanup(logger, CLUSTER_ID)
    logger.log_all("All done.")

if __name__ == "__main__":
    main()
