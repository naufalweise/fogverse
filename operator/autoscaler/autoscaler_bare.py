import subprocess
import yaml
import os
import argparse
from algorithms import Partitioner
from kubernetes import client, config

partitioner = Partitioner()  # TODO: masukan config awal sebagai parameter

# Static variables for namespace and Kafka resource name
NAMESPACE = "default"  # Replace with your namespace
KAFKA_RESOURCE_NAME = "my-cluster"  # Replace with your Kafka cluster name

def initialize_kubernetes():
    """Initialize Kubernetes configuration and return the API client."""
    config.load_kube_config()
    return client.CustomObjectsApi()

def patch_partitions(api_instance, partitions):
    """Patch the number of partitions for the Kafka cluster."""
    kafka_patch = {
        "spec": {
            "kafka": {
                "config": {
                    "num.partitions": partitions
                }
            }
        }
    }
    try:
        api_instance.patch_namespaced_custom_object(
            group="kafka.strimzi.io",
            version="v1beta2",
            namespace=NAMESPACE,
            plural="kafkas",
            name=KAFKA_RESOURCE_NAME,
            body=kafka_patch
        )
        print(f"Updated number of partitions to {partitions}")
    except client.exceptions.ApiException as e:
        print(f"Exception when patching Kafka partitions: {e}")

def scale_brokers(api_instance, brokers):
    """Scale the number of brokers for the Kafka cluster."""
    broker_patch = {
        "spec": {
            "kafka": {
                "replicas": brokers
            }
        }
    }
    try:
        api_instance.patch_namespaced_custom_object(
            group="kafka.strimzi.io",
            version="v1beta2",
            namespace=NAMESPACE,
            plural="kafkas",
            name=KAFKA_RESOURCE_NAME,
            body=broker_patch
        )
        print(f"Scaled brokers to {brokers}")
    except client.exceptions.ApiException as e:
        print(f"Exception when scaling brokers: {e}")

def check_threshold():
    """Check the scaling parameters and determine if scaling is needed and which metrics caused it."""
    # Replace with actual logic to check CPU usage
    cpu_usage = 0.75  # Placeholder value for CPU usage
    if cpu_usage > 0.8:  # Example threshold
        return (True,  "cpu")
    
    return (False, None)       

ALWAYS_SCALE_METRICS = ["cpu", "idle_thread"]  # Example metrics to check for scaling

def handle_scale(api_instance):
    """Main function to handle scaling logic."""
    consumers_count = 10
    brokers_count = 3
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
    patch_partitions(api_instance, P)
    scale_brokers(api_instance, b)

# Kubernetes API client
api_instance = initialize_kubernetes()
while True:
    handle_scale(api_instance)




