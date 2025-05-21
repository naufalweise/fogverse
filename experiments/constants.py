# Kafka node configuration constants.
CLUSTER_ID = "test-cluster"
NODE_PREFIX = "kafka-node"
CONTAINER_PREFIX = "test-container"
BASE_PORT = 9090
VOLUME_PREFIX = "kafka_test_data"
JOLOKIA_VERSION = "2.2.9"  # Or choose the latest stable version.
JOLOKIA_DOWNLOAD_URL = f"https://search.maven.org/remotecontent?filepath=org/jolokia/jolokia-agent-jvm/{JOLOKIA_VERSION}/jolokia-agent-jvm-{JOLOKIA_VERSION}-javaagent.jar"
JOLOKIA_AGENT_PATH = "/tmp/jolokia-jvm-2.2.9-agent.jar"  # Path to the Jolokia agent inside the container.
JOLOKIA_PORT_INTERNAL = 8778  # Port Jolokia listens on *inside* the container.
OPEN_FILES_LIMIT = 16_384
DISK_IO_LIMIT = 8_388_608  # 8 MiB/s

# Kafka cluster configuration constants.
TOPIC_NAME = "test-topic"
BOOTSTRAP_SERVER = "localhost:9092"

# Experiment constants.
NUM_RECORDS = 65_536  # This is arbitrary.
MBEAN_PRODUCE_REMOTETIME = "kafka.network:type=RequestMetrics,name=RemoteTimeMs,request=Produce"  # MBean for replication latency.
MESSAGE_SIZES = [1, 100]  # Message sizes in KB.
NUM_INSTANCES = 4  # Number of client instances to run.
KAFKA_HEAP_MIN = "-Xms2g"
KAFKA_HEAP_MAX = "-Xmx4g"
