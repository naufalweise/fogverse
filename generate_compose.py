import sys

def generate_docker_compose(n):
    base_yaml = """version: "3.8"

services:
"""
    volume_section = "volumes:\n"

    quorum_voters = ",".join([f"{i+1}@kafka-{i+1}:909{2*i+3}" for i in range(n)])

    for i in range(n):
        node_id = i + 1
        external_port = f"2909{node_id}"
        internal_port = f"909{2*node_id}"
        controller_port = f"909{2*node_id+1}"

        kafka_service = f"""  kafka-{node_id}:
    image: confluentinc/cp-kafka:latest
    container_name: kafka-broker-{node_id}
    environment:
      CLUSTER_ID: "test-cluster"
      KAFKA_NODE_ID: {node_id}
      KAFKA_PROCESS_ROLES: "controller,broker"
      KAFKA_LISTENERS: "INTERNAL://:{internal_port},EXTERNAL://:{external_port},CONTROLLER://:{controller_port}"
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://kafka-broker-{node_id}:{internal_port},EXTERNAL://localhost:{external_port}"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT"
      KAFKA_INTER_BROKER_LISTENER_NAME: "INTERNAL"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "{quorum_voters}"
      KAFKA_LOG_DIRS: "/var/lib/kafka/data"
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "{n}"
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "{n}"
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1"
    ports:
      - "{internal_port}:{internal_port}"
      - "{external_port}:{external_port}"
      - "{controller_port}:{controller_port}"
    volumes:
      - kafka_data_{node_id}:/var/lib/kafka/data
    mem_limit: 2g
    cpus: 1.0

"""
        base_yaml += kafka_service
        volume_section += f"  kafka_data_{node_id}:\n"

    return base_yaml + volume_section

if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 1  # Default to a single broker if no argument is given.
    with open("docker-compose.yml", "w") as f:
        f.write(generate_docker_compose(n))
    print(f"Generated docker-compose.yml with {n} Kafka brokers.")
