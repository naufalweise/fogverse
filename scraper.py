import requests
import time
import json
import re
import subprocess

# Define the Jolokia endpoints for the Kafka JMX metrics.
ENDPOINTS = {
    "TotalProduceRequestsPerSec": "http://localhost:9999/jolokia/read/kafka.server:type=BrokerTopicMetrics,name=TotalProduceRequestsPerSec",
    "FailedProduceRequestsPerSec": "http://localhost:9999/jolokia/read/kafka.server:type=BrokerTopicMetrics,name=FailedProduceRequestsPerSec",
    "TotalFetchRequestsPerSec": "http://localhost:9999/jolokia/read/kafka.server:type=BrokerTopicMetrics,name=TotalFetchRequestsPerSec",
    "FailedFetchRequestsPerSec": "http://localhost:9999/jolokia/read/kafka.server:type=BrokerTopicMetrics,name=FailedFetchRequestsPerSec"
}

def get_metric(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            # The metric value may be in MeanRate or OneMinuteRate. Use whichever is available.
            value = data.get("value", {})
            rate = value.get("OneMinuteRate")
            return float(rate) if rate is not None else 0.0
        else:
            print(f"Failed to get metric from {url}: HTTP {response.status_code}")
            return 0.0
    except Exception as e:
        print("Error getting metric:", e)
        return 0.0

def get_docker_stats(container_name):
    try:
        # Run docker stats in no-stream mode, output in JSON.
        result = subprocess.run(
            ["docker", "stats", container_name, "--no-stream", "--format", "{{json .}}"],
            capture_output=True,
            text=True,
            check=True
        )
        stats = json.loads(result.stdout.strip())
        # Extract CPU percentage. For example "150.00%" means 1.5 cores on a 2-core limit.
        cpu = stats.get("CPUPerc", "0%")
        cpu_value = float(re.sub(r"[%\s]+", "", cpu))
        return cpu_value
    except Exception as e:
        print("Error getting docker stats:", e)
        return None

def monitor():
    container = "kafka-broker"
    # If you have an average message size (in MB), set it here. For example, 1KB per message = 0.001 MB.
    # avg_message_size_mb = 0.001
    while True:
        prod_total = get_metric(ENDPOINTS["TotalProduceRequestsPerSec"])
        prod_failed = get_metric(ENDPOINTS["FailedProduceRequestsPerSec"])
        cons_total = get_metric(ENDPOINTS["TotalFetchRequestsPerSec"])
        cons_failed = get_metric(ENDPOINTS["FailedFetchRequestsPerSec"])
        
        # Calculate successful production and consumption rates (in requests per second).
        production_throughput = prod_total - prod_failed
        consumption_throughput = cons_total - cons_failed
        
        # Uncomment these lines if you want MB/s and have set avg_message_size_mb.
        # production_mb_s = production_throughput * avg_message_size_mb
        # consumption_mb_s = consumption_throughput * avg_message_size_mb
        
        cpu_usage = get_docker_stats(container)
        
        print("--- Kafka Metrics ---")
        print(f"Production Throughput (requests/s): {production_throughput:.2f}")
        # print(f"Production Throughput (MB/s): {production_mb_s:.4f}")
        print(f"Consumption Throughput (requests/s): {consumption_throughput:.2f}")
        # print(f"Consumption Throughput (MB/s): {consumption_mb_s:.4f}")
        if cpu_usage is not None:
            print(f"Container '{container}' CPU Usage: {cpu_usage:.2f}%")
        print("\n")
        time.sleep(1)

if __name__ == "__main__":
    monitor()
