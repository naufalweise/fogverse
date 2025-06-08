from typing import List, Optional, Dict
from kubernetes import client, config

class KafkaConsumerConfig:
    """Configuration for Kafka consumer."""
    def __init__(self, 
                 topics: List[str], 
                 bootstrap_servers: str, 
                 group_id: str,
                 offset_reset_policy: str = 'latest'
        ):
        self.topics = topics
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.offset_reset_policy = offset_reset_policy

class KedaScalerConfig:
    """Configuration for KEDA scaler."""
    def __init__(self, 
                 min_replicas: int = 0, 
                 max_replicas: int = 10, 
                 lag_threshold: int = 1,
                 cooldown_period: int = 5,
                 polling_interval: int = 10
        ):
        self.min_replicas = min_replicas
        self.max_replicas = max_replicas
        self.lag_threshold = lag_threshold
        self.cooldown_period = cooldown_period
        self.polling_interval = polling_interval

class KafkaKedaConsumer:
    """Kafka consumer with KEDA integration."""
    def __init__(self, 
                 name: str, 
                 image: str, 
                 consumer_config: KafkaConsumerConfig,
                 scaler_config: KedaScalerConfig, 
                 namespace: str = "default",
                 env_vars: Optional[Dict[str, str]] = None,
                 command: Optional[List[str]] = None,
                 args: Optional[List[str]] = None,
        ):
        """
        Args:
            name (str): Name of the deployment.
            image (str): Docker image to use for the consumer.
            consumer_config (KafkaConsumerConfig): Kafka consumer configuration.
            scaler_config (KedaScalerConfig): KEDA scaler configuration.
            namespace (str): Kubernetes namespace to deploy the resources.
            env_vars (Optional[Dict[str, str]]): Additional environment variables to set in the container.
                These variables will be merged with the default Kafka consumer environment variables.
        """
        self.name = name
        self.image = image
        self.consumer_config = consumer_config
        self.scaler_config = scaler_config
        self.namespace = namespace
        self.user_env_vars = env_vars or {}
        self.command = command
        self.args = args

        config.load_kube_config()
        self.api = client.AppsV1Api()
        self.keda_api = client.CustomObjectsApi()
        

    def deploy(self):
        """Deploy Kafka consumer and KEDA scaler."""
        try:
            self.api.create_namespaced_deployment(namespace=self.namespace, body=self._get_deployment())
            print(f"Deployment {self.name} created.")
            self.keda_api.create_namespaced_custom_object(
                group="keda.sh",
                version="v1alpha1",
                namespace=self.namespace,
                plural="scaledobjects",
                body=self._get_scaled_object(),
            )
            print(f"KEDA ScaledObject {self.name} created.")
        except client.exceptions.ApiException as e:
            print(f"Error: {e}")

    def _get_deployment(self):
        """Create Kubernetes Deployment for the Kafka consumer."""
        default_env = {
            "KAFKA_BOOTSTRAP_SERVERS": self.consumer_config.bootstrap_servers,
            "KAFKA_TOPIC": ",".join(self.consumer_config.topics),
            "GROUP_ID": self.consumer_config.group_id,
        }

        merged_env = {**default_env, **self.user_env_vars}
        env_list = [client.V1EnvVar(name=k, value=v) for k, v in merged_env.items()]

        return client.V1Deployment(
            metadata=client.V1ObjectMeta(name=self.name, namespace=self.namespace),
            spec=client.V1DeploymentSpec(
                replicas=1,
                selector=client.V1LabelSelector(match_labels={"app": self.name}),
                template=client.V1PodTemplateSpec(
                    metadata=client.V1ObjectMeta(labels={"app": self.name}),
                    spec=client.V1PodSpec(
                        containers=[
                            client.V1Container(
                                name=self.name,
                                image=self.image,
                                env=env_list,
                                command=self.command,
                                args=self.args,
                                resources=client.V1ResourceRequirements(
                                    limits={'memory': '500Mi', 'cpu': '500m'},
                                    requests={'memory': '500Mi', 'cpu': '500m'},
                                )
                            )
                        ]
                    ),
                ),
            ),
        )

    def _get_scaled_object(self):
        """Create KEDA ScaledObject to scale the Kafka consumer."""
        return {
            "apiVersion": "keda.sh/v1alpha1",
            "kind": "ScaledObject",
            "metadata": {"name": f"{self.name}-scaledobject", "namespace": self.namespace},
            "spec": {
                "scaleTargetRef": {"name": self.name},
                "minReplicaCount": self.scaler_config.min_replicas,
                "maxReplicaCount": self.scaler_config.max_replicas,
                "cooldownPeriod": self.scaler_config.cooldown_period,
                "pollingInterval": self.scaler_config.polling_interval,
                "triggers": [
                    {
                        "type": "apache-kafka",
                        "metadata": {
                            "bootstrapServers": self.consumer_config.bootstrap_servers,
                            "consumerGroup": self.consumer_config.group_id,
                            "topic": ",".join(self.consumer_config.topics),
                            "lagThreshold": str(self.scaler_config.lag_threshold),
                            "offsetResetPolicy": self.consumer_config.offset_reset_policy,
                        },
                    }
                ],
            },
        }
    
# Define Kafka consumer settings
consumer_config = KafkaConsumerConfig(
    topics=["exp"],
    bootstrap_servers="my-cluster-kafka-bootstrap:9092",
    group_id="cons_exp"
)

# Define KEDA scaling settings
scaler_config = KedaScalerConfig(
    min_replicas=0,
    max_replicas=10,
    lag_threshold=10
)

# Deploy Kafka consumer with KEDA
consumer = KafkaKedaConsumer(
    name="fogverse-processing",
    image="confluentinc/cp-kafka:7.5.0",
    consumer_config=consumer_config,
    scaler_config=scaler_config,
    namespace='kafka',
    command=['kafka-consumer-perf-test'],
    args=[
        '--bootstrap-server', "my-cluster-kafka-bootstrap:9092", 
        "--topic", 'exp',
        '--messages', '100000000',
        '--group', 'test',
        '--print-metrics' 
        ]
)

consumer.deploy()