from kubernetes import config, client

def get_container_cpu_percentage(namespace="default"):
    """
    Retrieves the current CPU usage percentage of containers in a given namespace.

    This requires the Metrics Server to be installed in the Kubernetes cluster
    and for resource limits to be set on the containers.

    Args:
        namespace (str): The Kubernetes namespace to inspect. Defaults to 'default'.

    Returns:
        dict: A dictionary where keys are pod names and values are dictionaries
              containing container names and their respective CPU usage percentage.
              Returns None if the Metrics API is not available or if CPU limits
              are not set.
    """
    try:
        config.load_kube_config()
        core_v1 = client.CoreV1Api()
        metrics_api = client.CustomObjectsApi()

        pod_metrics = metrics_api.list_namespaced_custom_object(
            group="metrics.k8s.io",
            version="v1beta1",
            namespace=namespace,
            plural="pods",
        )

        usage_percentage = {}
        for item in pod_metrics["items"]:
            pod_name = item["metadata"]["name"]
            containers_usage = {}
            pod_info = core_v1.read_namespaced_pod(name=pod_name, namespace=namespace)

            for container_status in pod_info.status.container_statuses:
                container_name = container_status.name
                limit_cores = None
                for container_spec in pod_info.spec.containers:
                    if container_spec.name == container_name and container_spec.resources and container_spec.resources.limits and container_spec.resources.limits.get("cpu"):
                        limit_cores = container_spec.resources.limits["cpu"]
                        break

                if limit_cores is None:
                    containers_usage[container_name] = "N/A (CPU limit not set)"
                    continue

                container_metrics = next(
                    (c for c in item["containers"] if c["name"] == container_name),
                    None,
                )

                if container_metrics and container_metrics["usage"].get("cpu"):
                    cpu_usage = container_metrics["usage"]["cpu"]
                    cpu_milli = 0
                    if cpu_usage.endswith("n"):
                        cpu_milli = int(cpu_usage[:-1]) / 1_000_000
                    elif cpu_usage.endswith("m"):
                        cpu_milli = int(cpu_usage[:-1])
                    else:
                        cpu_milli = int(float(cpu_usage) * 1000)

                    limit_milli = 0
                    if limit_cores.endswith("m"):
                        limit_milli = int(limit_cores[:-1])
                    else:
                        limit_milli = int(float(limit_cores) * 1000)

                    if limit_milli > 0:
                        percentage = (cpu_milli / limit_milli) * 100
                        containers_usage[container_name] = f"{percentage:.2f}%"
                    else:
                        containers_usage[container_name] = "N/A (CPU limit is zero)"
                else:
                    containers_usage[container_name] = "N/A (Usage data not found)"

            usage_percentage[pod_name] = containers_usage

        return usage_percentage

    except client.ApiException as e:
        if e.status == 404:
            print(f"Error: Metrics API not found. Ensure Metrics Server is installed in the cluster.")
        else:
            print(f"Error retrieving metrics: {e}")
        return None
    except config.ConfigException:
        print("Could not load Kubernetes configuration. Ensure your kubeconfig is properly set up.")
        return None

if __name__ == "__main__":
    cpu_percentages = get_container_cpu_percentage(namespace="kafka")  # Replace with your namespace if needed
    if cpu_percentages:
        for pod, containers in cpu_percentages.items():
            print(f"Pod: {pod}")
            for container, percentage in containers.items():
                print(f"  Container: {container}, CPU Usage: {percentage}")