import sys

from experiments.constants import CLUSTER_ID, CONTAINER_PREFIX, NODE_PREFIX, VOLUME_PREFIX

JOLOKIA_VERSION = "1.7.2"  # Or choose the latest stable version
JOLOKIA_PORT_INTERNAL = 8778 # Port Jolokia listens on *inside* the container

def generate_docker_compose(n, base_port=9090):
    """
    Generates a docker-compose file for n Kafka nodes with automated Jolokia setup.
    Node IDs start at 0 and increment by 2.
    Internal listener port ends with 0, external ends with 2, controller ends with 4.
    Jolokia mapped host port will end with 6.
    """

    yaml_parts = ['version: "3.8"\n\nservices:\n']
    volumes_parts = ['\nvolumes:\n']

    quorum_voters = ",".join([
        f"{i * 2}@{NODE_PREFIX}-{i * 2}:{base_port + (10 * i) + 4}" for i in range(n)
    ])

    for i in range(n):
        node_id = i * 2
        ports = {
            'internal': base_port + (10 * i),
            'external': base_port + (10 * i) + 2,
            'controller': base_port + (10 * i) + 4,
            'jolokia_host': base_port + (10 * i) + 6 # Host port mapped to Jolokia
        }

        # Preserve existing KAFKA_OPTS and append Jolokia agent
        # The command will handle the download and setting of KAFKA_OPTS
        
        jolokia_download_and_run_script = f"""\
bash -c "
  echo 'Node {node_id}: Checking for curl...'
  if ! command -v curl &> /dev/null; then
    echo 'Node {node_id}: curl not found. Attempting to install...'
    if command -v microdnf &> /dev/null; then
      microdnf install -y curl
    elif command -v apt-get &> /dev/null; then
      apt-get update && apt-get install -y curl
    else
      echo 'Node {node_id}: Cannot install curl. Please ensure curl is in the base image.' >&2
      exit 1
    fi
  fi

  JOLOKIA_AGENT_PATH=/tmp/jolokia-jvm-{JOLOKIA_VERSION}-agent.jar
  if [ ! -f $$JOLOKIA_AGENT_PATH ]; then
    echo 'Node {node_id}: Downloading Jolokia agent {JOLOKIA_VERSION}...'
    curl -L -s -o $$JOLOKIA_AGENT_PATH \\
      https://search.maven.org/remotecontent?filepath=org/jolokia/jolokia-jvm/{JOLOKIA_VERSION}/jolokia-jvm-{JOLOKIA_VERSION}-agent.jar
    if [ $$? -ne 0 ] || [ ! -s $$JOLOKIA_AGENT_PATH ]; then
      echo 'Node {node_id}: Failed to download Jolokia agent.' >&2
      # Try an alternative URL if the primary fails (e.g., from Maven Central directly)
      echo 'Node {node_id}: Trying alternative download from Maven Central...'
      curl -L -s -o $$JOLOKIA_AGENT_PATH \\
        https://repo1.maven.org/maven2/org/jolokia/jolokia-jvm/{JOLOKIA_VERSION}/jolokia-jvm-{JOLOKIA_VERSION}-agent.jar
      if [ $$? -ne 0 ] || [ ! -s $$JOLOKIA_AGENT_PATH ]; then
        echo 'Node {node_id}: Failed to download Jolokia agent from alternative URL. Exiting.' >&2
        exit 1
      fi
    fi
    echo 'Node {node_id}: Jolokia agent downloaded successfully.'
  else
    echo 'Node {node_id}: Jolokia agent already exists.'
  fi

  # Prepend Jolokia agent to KAFKA_OPTS. $${{VAR}} is for docker-compose delayed interpolation for existing env vars.
  # However, KAFKA_OPTS is often set by the image's entrypoint scripts after env vars are sourced.
  # A common way is to ensure KAFKA_OPTS gets this value.
  # The confluentinc/cp-kafka images typically source a file that sets KAFKA_OPTS.
  # We will export it so the subsequent kafka run script picks it up.
  
  export KAFKA_OPTS=\\\"$${{KAFKA_OPTS:-}} -javaagent:$$JOLOKIA_AGENT_PATH=port={JOLOKIA_PORT_INTERNAL},host=0.0.0.0,discoveryEnabled=false\\\"  echo \\\"Node {node_id}: Augmented KAFKA_OPTS: $$KAFKA_OPTS\\\"

  # Execute the original Kafka entrypoint/command
  /etc/confluent/docker/run
"
"""
        # Note: Escaping for YAML and shell script:
        # $$ for Docker Compose to pass a single $ to the shell.
        # \" to embed quotes within the KAFKA_OPTS string for the shell.
        # \\\" for YAML to represent \" in the final shell command.
        # discoveryEnabled=false is good practice for security and to prevent multicast traffic.

        kafka_service = f"""  {NODE_PREFIX}-{node_id}:
    image: confluentinc/cp-kafka:latest
    container_name: {CONTAINER_PREFIX}-{node_id}
    command: >
{indent_string(jolokia_download_and_run_script, 6)}
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
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true" # Good for testing
      KAFKA_DELETE_TOPIC_ENABLE: "true" # Good for cleaning up test topics
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "{min(n, 3)}" # Adjusted to min(n,3)
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "{min(n, 3)}" # Adjusted
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "{max(1, min(n, 3) -1 if n > 1 else 1)}" # Adjusted
      # KAFKA_OPTS: "" # Initial KAFKA_OPTS can be set here if needed, command script will append
    labels:
      cluster_id: "{CLUSTER_ID}"
    ports:
      - "{ports['internal']}:{ports['internal']}"
      - "{ports['external']}:{ports['external']}"
      - "{ports['controller']}:{ports['controller']}"
      - "{ports['jolokia_host']}:{JOLOKIA_PORT_INTERNAL}" # Map Jolokia port
    volumes:
      - {VOLUME_PREFIX}_{node_id}:/var/lib/kafka/data
    mem_limit: 2g 
    cpus: 1.0
    healthcheck:
      test: ["CMD-SHELL", "curl -f http://localhost:{JOLOKIA_PORT_INTERNAL}/jolokia/read/kafka.server:type=BrokerState/BrokerState || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 10
      start_period: 20s

"""
        yaml_parts.append(kafka_service)
        volumes_parts.append(f"  {VOLUME_PREFIX}_{node_id}:\n    labels:\n      cluster_id: \"{CLUSTER_ID}\"\n")

    return ''.join(yaml_parts + volumes_parts)

def indent_string(text, num_spaces):
    return "\n".join(" " * num_spaces + line for line in text.splitlines())

if __name__ == "__main__":
    num_brokers = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    output_file = "docker-compose.yml" #  Was "experiments/docker-compose.yml"

    # Create experiments directory if it doesn't exist
    # import os
    # os.makedirs("experiments", exist_ok=True)
    
    with open(output_file, "w") as f:
        f.write(generate_docker_compose(num_brokers))

    print(f"Generated {output_file} with {num_brokers} Kafka brokers.")
    print(f"Jolokia will be accessible on host ports ending with ...6 (e.g., 9096, 9106, ... for brokers 0, 2, ... respectively).")
    print(f"Internal Jolokia port in containers: {JOLOKIA_PORT_INTERNAL}")