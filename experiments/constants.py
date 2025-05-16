# Kafka node configuration constants.
CLUSTER_ID = "test-cluster"
NODE_PREFIX = "kafka-node"
CONTAINER_PREFIX = "test-container"
VOLUME_PREFIX = "kafka_test_data"
JOLOKIA_VERSION = "1.7.2"  # Or choose the latest stable version.
JOLOKIA_PORT_INTERNAL = 8778  # Port Jolokia listens on *inside* the container.

# Kafka cluster configuration constants.
TOPIC_NAME = "test-topic"
NODE_ID_BASE = 0
BROKER_ADDRESS = "localhost:9092"

# Experiment constants.
FIRST_CONTAINER = f"{CONTAINER_PREFIX}-{NODE_ID_BASE}"
NUM_RECORDS = 65_536  # This is arbitrary.
