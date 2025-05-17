import sys

from experiments.constants import BASE_PORT, CLUSTER_ID, CONTAINER_PREFIX, JOLOKIA_PORT_INTERNAL, NODE_PREFIX, VOLUME_PREFIX

def generate_docker_compose(n, base_port=BASE_PORT):
    """
    Generates a docker-compose file for n Kafka nodes. Node IDs start at 0 and increment by 2.
    Within the ports allocated per node, the internal listener port effectively ends with 0, the external ends with 2, and the controller ends with 4.
    Jolokia mapped host port will end with 6.
    """

    yaml = ['version: "3.8"\n\nservices:\n']
    volumes = ['volumes:\n']

    quorum_voters = ",".join([
        f"{i * 2}@{NODE_PREFIX}-{i * 2}:{base_port + (10 * i) + 4}" for i in range(n)
    ])

    for i in range(n):
        node_id = i * 2
        ports = {
            'internal': base_port + (10 * i),
            'external': base_port + (10 * i) + 2,
            'controller': base_port + (10 * i) + 4,
            'jolokia_host': base_port + (10 * i) + 6
        }

        kafka_service = f"""  {NODE_PREFIX}-{node_id}:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: {CONTAINER_PREFIX}-{node_id}
    environment:
      CLUSTER_ID: "{CLUSTER_ID}"
      KAFKA_NODE_ID: {node_id}
      KAFKA_PROCESS_ROLES: "controller,broker"
      KAFKA_LISTENERS: "INTERNAL://:{ports['internal']},EXTERNAL://:{ports['external']},CONTROLLER://:{ports['controller']}"
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://{CONTAINER_PREFIX}-{node_id}:{ports['internal']},EXTERNAL://localhost:{ports['external']}"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT"
      KAFKA_INTER_BROKER_LISTENER_NAME: "INTERNAL"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "{quorum_voters}"
      KAFKA_LOG_DIRS: "/var/lib/kafka/data"
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_DELETE_TOPIC_ENABLE: "true"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "{min(n, 3)}"
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "{min(n, 3)}"
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "{max(1, min(n, 3) -1 if n > 1 else 1)}"
    labels:
      cluster_id: "{CLUSTER_ID}"
    ports:
      - "{ports['internal']}:{ports['internal']}"
      - "{ports['external']}:{ports['external']}"
      - "{ports['controller']}:{ports['controller']}"
      - "{ports['jolokia_host']}:{JOLOKIA_PORT_INTERNAL}"
    volumes:
      - {VOLUME_PREFIX}_{node_id}:/var/lib/kafka/data
    mem_limit: 2g
    cpus: 1.0
    healthcheck:
      test: ["CMD-SHELL", "curl -f http://localhost:{JOLOKIA_PORT_INTERNAL}/jolokia/read/kafka.server:type=BrokerState/BrokerState || exit 1"]
      interval: 8s
      timeout: 4s
      retries: 8
      start_period: 20s

"""
        yaml.append(kafka_service)
        volumes.append(f"  {VOLUME_PREFIX}_{node_id}:\n    labels:\n      cluster_id: \"{CLUSTER_ID}\"\n")

    return ''.join(yaml + volumes)

if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 1

    with open("docker-compose.yml", "w") as f:
        f.write(generate_docker_compose(n))

    print(f"Generated docker-compose.yml with {n} Kafka brokers.")
