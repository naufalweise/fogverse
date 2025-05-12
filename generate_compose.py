import sys

def generate_docker_compose(n, base_port=9090):
    """
    Generates a docker-compose file for n Kafka nodes.Node IDs start at 0 and increment by 2.
    Within the ports allocated per node, the internal listener port effectively ends with 0, the external ends with 2, and the controller ends with 4.
    """

    yaml = ['version: "3.8"\n\nservices:\n']
    volumes = ['volumes:\n']

    quorum_voters = ",".join([
        f"{i * 2}@kafka-node-{i * 2}:{base_port + (10 * i) + 4}" for i in range(n)
    ])

    for i in range(n):
        node_id = i * 2
        ports = {
            'internal': base_port + (10 * i),
            'external': base_port + (10 * i) + 2,
            'controller': base_port + (10 * i) + 4
        }

        kafka_service = f"""  kafka-node-{node_id}:
    image: confluentinc/cp-kafka:latest
    container_name: test-container-{node_id}
    environment:
      CLUSTER_ID: "test-cluster"
      KAFKA_NODE_ID: {node_id}
      KAFKA_PROCESS_ROLES: "controller,broker"
      KAFKA_LISTENERS: "INTERNAL://:{ports['internal']},EXTERNAL://:{ports['external']},CONTROLLER://:{ports['controller']}"
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://test-container-{node_id}:{ports['internal']},EXTERNAL://localhost:{ports['external']}"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT"
      KAFKA_INTER_BROKER_LISTENER_NAME: "INTERNAL"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "{quorum_voters}"
      KAFKA_LOG_DIRS: "/var/lib/kafka/data"
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "{min(n, 5)}"
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "{min(n, 5)}"
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "{min(n, 5)}"
    ports:
      - "{ports['internal']}:{ports['internal']}"
      - "{ports['external']}:{ports['external']}"
      - "{ports['controller']}:{ports['controller']}"
    volumes:
      - kafka_test_data_{node_id}:/var/lib/kafka/data
    mem_limit: 2g
    cpus: 1.0

"""
        yaml.append(kafka_service)
        volumes.append(f"  kafka_test_data_{node_id}:\n")

    return ''.join(yaml + volumes)

if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 1

    with open("docker-compose.yml", "w") as f:
        f.write(generate_docker_compose(n))

    print(f"Generated docker-compose.yml with {n} Kafka brokers.")
