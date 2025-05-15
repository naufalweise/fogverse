# Kafka node configuration constants.
CLUSTER_ID = "test-cluster"
NODE_PREFIX = "kafka-node"
CONTAINER_PREFIX = "test-container"
VOLUME_PREFIX = "kafka_test_data"

# Kafka cluster configuration constants.
TOPIC_NAME = "test-topic"
NODE_ID_BASE = 0
BROKER_ADDRESS = "localhost:9092"
MESSAGE_SIZE = 16_384  # 16 KiB.

# Experiment constants.
FIRST_CONTAINER = f"{CONTAINER_PREFIX}-{NODE_ID_BASE}"
PROD_THROUGHPUT = "prod_throughput"
