import kopf
import requests
import subprocess
import yaml
import time
from kubernetes import client, config
import kubernetes

PROMETHEUS_URL = "http://prometheus.monitoring.svc.cluster.local:9090"
KAFKA_CLUSTER_NAME = "my-cluster"
KAFKA_NAMESPACE = "kafka"
MIN_REPLICAS = 3
MAX_REPLICAS = 6

def get_cpu_usage():
    pattern = f"{KAFKA_CLUSTER_NAME}-kafka-[0-9]+"
    query = f'avg(rate(container_cpu_usage_seconds_total{{pod=~"{pattern}"}}[2m])) * 100'
    resp = requests.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": query})
    result = resp.json()['data']['result']
    if not result:
        return 0
    return float(result[0]['value'][1])


# Load cluster configuration
config.load_incluster_config()  # use load_kube_config() for local testing

custom_api = client.CustomObjectsApi()

def get_current_replicas():
    kafka_cr = custom_api.get_namespaced_custom_object(
        group="kafka.strimzi.io",
        version="v1beta2",  # or "v1beta1" depending on your Strimzi version
        namespace=KAFKA_NAMESPACE,
        plural="kafkas",
        name=KAFKA_CLUSTER_NAME
    )
    return kafka_cr["spec"]["kafka"]["replicas"]

def patch_replicas(new_replicas):
    patch = {
        "spec": {
            "kafka": {
                "replicas": new_replicas
            }
        }
    }
    custom_api.patch_namespaced_custom_object(
        group="kafka.strimzi.io",
        version="v1beta2",
        namespace=KAFKA_NAMESPACE,
        plural="kafkas",
        name=KAFKA_CLUSTER_NAME,
        body=patch
    )
    print(f"⚙️ Scaled Kafka brokers to {new_replicas}")


@kopf.timer('kafka.strimzi.io', interval=60.0)
def kafka_scaler(spec, **_):
    cpu = get_cpu_usage()
    print(f"📊 CPU usage: {cpu:.2f}%")
    current_replicas = get_current_replicas()

    if cpu > 80 and current_replicas < MAX_REPLICAS:
        patch_replicas(current_replicas + 1)
    elif cpu < 30 and current_replicas > MIN_REPLICAS:
        patch_replicas(current_replicas - 1)
    else:
        print("✅ No scaling required.")
