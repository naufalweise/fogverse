# Fogverse Kafka KEDA Consumer

This library simplifies the deployment of Kafka consumers with KEDA autoscaling in a Kubernetes environment. It abstracts the complexity of containerizing your Kafka consumer and generating the necessary Kubernetes Deployment and KEDA ScaledObject YAML definitions.

## Prerequisites

1. **Kubernetes Environment**  
   Ensure you have a running Kubernetes cluster.

2. **KEDA Installation**  
   Install KEDA in your Kubernetes cluster by running:

   ```sh
   helm repo add kedacore https://kedacore.github.io/charts
   helm repo update
   helm install keda kedacore/keda --namespace keda --create-namespace
   ```

## Consumer Logic Example
You can use any Kafka consumer library. Here’s an example using the fogverse library:

```python
import asyncio
from fogverse import KafkaConsumer

from config.constants import (
    KAFKA_BOOTSTRAP_SERVERS, 
    KAFKA_TOPIC,
    GROUP_ID
)

async def main():
    # Create the consumer with the appropriate configuration.
    # You can pass additional parameters like group_id and auto_offset_reset here.
    consumer = KafkaConsumer(
        topics=[KAFKA_TOPIC],
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID
    )

    await consumer.start()
    print(f"Listening for messages on topic: {KAFKA_TOPIC}")

    try:
        while True:
            # Wait for a new message asynchronously.
            msg = await consumer.receive()
            print("Received Message:", msg)

            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping Kafka Consumer...")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(main())
```

    Note: The above consumer code is containerized and available on Docker Hub at anindyalkwr/fogverse_processing:latest.

## Example Usage with Library Abstractions
The library provides a simplified interface to deploy your Kafka consumer with KEDA without manually applying YAML files. For example:

```python
# Define Kafka consumer settings
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

# Deploy Kafka consumer with KEDA
consumer = KafkaKedaConsumer(
    name="fogverse-processing",
    image="anindyalkwr/fogverse_processing:latest",
    consumer_config=consumer_config,
    scaler_config=scaler_config
)

consumer.deploy()
```
This approach reduces the need to manually generate and apply YAML files by automatically creating the necessary Kubernetes resources (Deployment and ScaledObject).

## Environment Variables and Secret Management
Secrets:
The library leverages Kubernetes Secrets to manage sensitive environment variables such as group_id, bootstrap_servers, and topic.

Default Behavior:
If the secret values do not match the required configuration, the library will either fall back to default values or throw an error. Ensure your Kubernetes secrets are correctly set up to avoid misconfigurations.

Contributing
Contributions are welcome! Feel free to fork the repository, open issues, or submit pull requests with enhancements or bug fixes.