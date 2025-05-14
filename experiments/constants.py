# Kafka node configuration constants.
CLUSTER_ID = "test-cluster"
NODE_PREFIX = "kafka-node"
CONTAINER_PREFIX = "test-container"
VOLUME_PREFIX = "kafka_test_data"

# Kafka cluster configuration constants.
TOPIC_NAME = "test-topic"
BROKER_ADDRESS = "localhost:9092"
NODE_ID_BASE = 0

# Experiment constants.
FIRST_CONTAINER = f"{CONTAINER_PREFIX}-{NODE_ID_BASE}"
PROD_THROUGHPUT = "prod_throughput"
