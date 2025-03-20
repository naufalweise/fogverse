from fogverse.auto_scaling.keda_consumer.keda_consumer import KafkaConsumerConfig, KafkaKedaConsumer, KedaScalerConfig

consumer_config = KafkaConsumerConfig(
    topics=["sensor-logs"],
    bootstrap_servers="host.docker.internal:9094",
    group_id="scaling_sensor_group"
)

# Define KEDA scaling settings
scaler_config = KedaScalerConfig(
    min_replicas=0,
    max_replicas=10,
    lag_threshold=10
)

# Define additional environment variables if needed
additional_env_vars = {
    "CUSTOM_VAR": "custom_value",
    "ANOTHER_VAR": "another_value"
}

# Deploy Kafka consumer with KEDA, including custom env variables
consumer = KafkaKedaConsumer(
    name="fogverse-processing",
    image="anindyalkwr/fogverse_processing:latest",
    consumer_config=consumer_config,
    scaler_config=scaler_config,
    namespace="default",
    env_vars=additional_env_vars
)

consumer.deploy()
