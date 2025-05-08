import subprocess
import yaml
import os
import argparse
from algorithms import Partitioner
from kubernetes import client, config
import get_cpu

partitioner = Partitioner()  # TODO: masukan config awal sebagai parameter

# Static variables for KAFKA_NAMESPACE and Kafka resource name
KAFKA_NAMESPACE = "kafka"  # Replace with your KAFKA_NAMESPACE
KAFKA_CLUSTER_NAME = "my-cluster"  # Replace with your Kafka cluster name
PROMETHEUS_URL = "http://localhost:9090"  # Replace with your Prometheus server URL

def initialize_kubernetes():
    """Initialize Kubernetes configuration and return the API client."""
    config.load_kube_config()
    return client.CustomObjectsApi()

TOPIC_NAME = "my-topic-1" 
def patch_partitions(api_instance, partitions):
    """Patch the number of partitions for the Kafka cluster."""
    kafka_patch = {
        "spec": {
            "partitions": partitions
        }
    }
    try:
        api_instance.patch_namespaced_custom_object(
            group="kafka.strimzi.io",
            version="v1beta2",
            namespace=KAFKA_NAMESPACE,
            plural="kafkatopics",
            name=TOPIC_NAME,
            body=kafka_patch
        )
        print(f"Updated number of partitions to {partitions}")
    except client.exceptions.ApiException as e:
        print(f"Exception when patching Kafka partitions: {e}")

def scale_brokers(api_instance, brokers):
    """Scale the number of brokers for the Kafka cluster."""
    try:
        # Define the group, version, and plural for the Kafka Nodepool
        group = "kafka.strimzi.io"
        version = "v1beta2"
        plural = "kafkanodepools"

        # Construct the body for the scale operation.
        body = {
            "spec": {
                "replicas": brokers
            }
        }
        # Patch the Kafka Nodepool
        api_response = api_instance.patch_namespaced_custom_object(
            group=group,
            version=version,
            namespace=KAFKA_NAMESPACE,
            plural=plural,
            name="broker",
            body=body,
        )
        print(f"Scaled brokers to {brokers}")
    except client.exceptions.ApiException as e:
        print(f"Exception when scaling brokers: {e}")

def get_request_handler_idle_ratio():
    """Fetch the request handler idle ratio of Kafka brokers from Prometheus."""
    query = 'kafka_server_request_handler_avg_idle_percent'  # Replace with the correct Prometheus query for idle ratio
    try:
        response = requests.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": query})
        response.raise_for_status()
        data = response.json()
        # Extract the average idle ratio from the Prometheus response
        idle_ratios = [float(result["value"][1]) for result in data["data"]["result"]]
        if idle_ratios:
            return sum(idle_ratios) / len(idle_ratios)  # Return the average idle ratio
        else:
            print("No idle ratio data available.")
            return 1.0 # Default to 0 if no data is available
    except requests.exceptions.RequestException as e:
        print(f"Error fetching request handler idle ratio from Prometheus: {e}")
        return 1.0 # Default to 0 if the query fails


def get_kafka_brokers_cpu_usage():
    """Fetch the CPU usage of Kafka brokers from the Kubernetes metrics API."""
    try:
        pod_cpus = get_cpu.get_pod_average_cpu_percentage(KAFKA_NAMESPACE)

        cpu_usages = [pod_data['average_cpu_percentage'] for pod_data in pod_cpus if is_kafka_broker(pod_data)]
        return calculate_average_cpu_usage(cpu_usages)
    except client.exceptions.ApiException as e:
        print(f"Error fetching CPU usage from Kubernetes metrics API: {e}")
        return 0.0

def is_kafka_broker(pod):
    """Check if the pod is a Kafka broker."""
    return "broker" in pod['pod_name']  


def calculate_average_cpu_usage(cpu_usages):
    """Calculate the average CPU usage."""
    if cpu_usages:
        return sum(cpu_usages) / len(cpu_usages)
    print("No CPU usage data available for Kafka brokers.")
    return 0.0

def check_threshold():
    """Check the scaling parameters and determine if scaling is needed and which metrics caused it."""
    #cpu_usage = get_kafka_brokers_cpu_usage()
    cpu_usage = 100
    if cpu_usage > 80:
        return (True,  "cpu")
    idle_thread = get_request_handler_idle_ratio()

    if idle_thread < 0.2:
        return (True, "idle_thread")
    return (False, None)       

ALWAYS_SCALE_METRICS = ["cpu", "idle_thread"]  # Example metrics to check for scaling


import requests  # Add this import for Prometheus API calls


def get_consumers_count():
    """Fetch the Kafka consumers count from Prometheus."""
    query = 'kafka_consumer_group_current_offset'  # Replace with the correct Prometheus query for Kafka consumers
    try:
        response = requests.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": query})
        response.raise_for_status()
        data = response.json()
        # Extract the consumers count from the Prometheus response
        consumers_count = len(data["data"]["result"])
        return consumers_count
    except requests.exceptions.RequestException as e:
        print(f"Error fetching consumers count from Prometheus: {e}")
        return 0  # Default to 0 if the query fails


def get_brokers_count():
    kafka_cr = custom_api.get_namespaced_custom_object(
        group="kafka.strimzi.io",
        version="v1beta2",  # or "v1beta1" depending on your Strimzi version
        namespace=KAFKA_NAMESPACE,
        plural="kafkanodepools",
        name='broker'
    )
    return kafka_cr["spec"]["replicas"]


def handle_scale(custom_api):
    """Main function to handle scaling logic."""
    consumers_count = get_consumers_count()
    brokers_count = get_brokers_count()
    throughput = 100e6
    should_scale, trigger = check_threshold()
    if not should_scale:
        print("No scaling needed.")
        return
    
    print(f"Scaling triggered by {trigger}")

    (P, b) = partitioner.bromin_scale(c=consumers_count, b=brokers_count, t=throughput, always_scale=trigger in ALWAYS_SCALE_METRICS)

    if P == -1 or b == -1:
        print("No feasible solution found for scaling.")
        return
    # Patch partitions and scale brokers
    print(f"Scaling to {P} partitions and {b} brokers")
    patch_partitions(custom_api, P)
    scale_brokers(custom_api, b)

# Kubernetes API client
custom_api = initialize_kubernetes()
import time
interval = 1  # Check every 60 seconds
while True:
    # Sleep for the specified interval
    time.sleep(interval)
    handle_scale(custom_api)




