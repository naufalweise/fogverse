from kubernetes import config, client
def parse_cpu_usage(cpu_usage):
    """
    Parses CPU usage from a string to an integer in milli cores.

    Args:
        cpu_usage (str): The CPU usage string (e.g., "100m", "1", "1000m").

    Returns:
        int: The CPU usage in milli cores.
    """
    if cpu_usage.endswith("n"):
        return int(cpu_usage[:-1]) // 1_000_000
    elif cpu_usage.endswith("m"):
        return int(cpu_usage[:-1])
    else:
        return int(float(cpu_usage) * 1000)

def get_pod_average_cpu_percentage(namespace="default"):
    """
    Retrieves the average CPU usage percentage for each pod in a given namespace.

    Requires the Metrics Server to be installed in the Kubernetes cluster and
    for CPU limits to be defined for the containers.

    Args:
        namespace (str): The Kubernetes namespace to inspect. Defaults to 'default'.

    Returns:
        list: A list of dictionaries, where each dictionary contains:
            - "pod_name" (str): The name of the pod.
            - "average_cpu_percentage" (float): The average CPU usage
              percentage across all containers in the pod. Min 0, max 100.
              Returns an empty list if no data is available or an error occurs.
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

        pod_list = []
        for item in pod_metrics["items"]:
            pod_name = item["metadata"]["name"]
            total_cpu_percentage = 0
            container_count = 0
            pod_info = core_v1.read_namespaced_pod(name=pod_name, namespace=namespace)

            for container_status in pod_info.status.container_statuses:
                container_name = container_status.name
                limit_cores = None

                for container_spec in pod_info.spec.containers:
                    if (
                        container_spec.name == container_name
                        and container_spec.resources
                        and container_spec.resources.limits
                        and container_spec.resources.limits.get("cpu")
                    ):
                        limit_cores = container_spec.resources.limits["cpu"]
                        break

                if limit_cores is None:
                    continue  # Skip this container if no CPU limit is set

                container_metrics = next(
                    (c for c in item["containers"] if c["name"] == container_name), None
                )
                if container_metrics and container_metrics["usage"].get("cpu"):
                    cpu_usage = container_metrics["usage"]["cpu"]
                    cpu_milli = parse_cpu_usage(cpu_usage)
                    limit_milli = parse_cpu_usage(limit_cores)

                    if limit_milli > 0:
                        container_count += 1
                        total_cpu_percentage += (cpu_milli / limit_milli) * 100

            if container_count > 0:
                average_cpu_percentage = total_cpu_percentage / container_count
                pod_list.append(
                    {
                        "pod_name": pod_name,
                        "average_cpu_percentage": average_cpu_percentage,
                    }
                )
            elif container_count == 0:
                pod_list.append(
                    {
                        "pod_name": pod_name,
                        "average_cpu_percentage": 0,
                    }
                )

        return pod_list

    except client.ApiException as e:
        if e.status == 404:
            print(
                "Error: Metrics API not found. Ensure Metrics Server is installed in the cluster."
            )
        else:
            print(f"Error retrieving metrics: {e}")
        return []
    except config.ConfigException:
        print("Could not load Kubernetes configuration. Ensure your kubeconfig is properly set up.")
        return []
    except KeyError as e:
        print(
            f"Error: Key {e} not found in metrics data. Check the structure of the"
            " metrics.k8s.io/v1beta1/pods response."
        )
        return []

if __name__ == "__main__":
    pod_cpu_usage_list = get_pod_average_cpu_percentage(
        namespace="kafka"
    )  # Replace with your namespace
    if pod_cpu_usage_list:
        for pod_data in pod_cpu_usage_list:
            print(
                f"Pod Name: {pod_data['pod_name']}, Average CPU Usage:"
                f" {pod_data['average_cpu_percentage']:.2f}%"
            )
    else:
        print("No pod CPU usage data available.")
