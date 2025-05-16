import time
import json
import argparse
import requests # pip install requests
from kafka.admin import KafkaAdminClient, NewTopic # pip install kafka-python
from kafka.producer import KafkaProducer # pip install kafka-python
from kafka.errors import TopicAlreadyExistsError, KafkaError, NoBrokersAvailable

# Constants from your docker-compose generator (or pass them)
NODE_PREFIX = "kafka-node" # Should match generate_docker_compose
JOLOKIA_PORT_INTERNAL = 8778 # Used in KAFKA_OPTS
MBEAN_PRODUCE_REMOTETIME = "kafka.network:type=RequestMetrics,name=RemoteTimeMs,request=Produce"

# Global list to store external listener ports for Kafka brokers
KAFKA_BROKER_EXTERNAL_PORTS = []

def setup_kafka_broker_ports(num_brokers, base_kafka_port=9090):
    global KAFKA_BROKER_EXTERNAL_PORTS
    KAFKA_BROKER_EXTERNAL_PORTS = []
    for i in range(num_brokers):
        KAFKA_BROKER_EXTERNAL_PORTS.append(base_kafka_port + (10 * i) + 2)

def get_jolokia_urls(num_brokers, base_kafka_port=9090):
    urls = []
    for i in range(num_brokers):
        node_id = i * 2
        jolokia_host_port = base_kafka_port + (10 * i) + 6
        urls.append(f"http://localhost:{jolokia_host_port}/jolokia")
    return urls

def get_bootstrap_servers():
    if not KAFKA_BROKER_EXTERNAL_PORTS:
        raise ValueError("Kafka broker external ports not set up. Call setup_kafka_broker_ports first.")
    return [f"localhost:{port}" for port in KAFKA_BROKER_EXTERNAL_PORTS]

def create_test_topic(admin_client, topic_name, num_brokers, replication_factor_config):
    effective_replication_factor = min(num_brokers, replication_factor_config)
    topic = NewTopic(
        name=topic_name,
        num_partitions=1, # Single partition simplifies leader identification
        replication_factor=effective_replication_factor
    )
    try:
        print(f"Attempting to create topic '{topic_name}' with RF={effective_replication_factor}...")
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{topic_name}' created or already exists.")
        
        # Wait for topic to be fully created and leader elected
        for _ in range(10): # Retry for a few seconds
            metadata = admin_client.describe_topics([topic_name])
            if metadata and metadata[0]['partitions'] and metadata[0]['partitions'][0]['leader'] != -1:
                leader_id = metadata[0]['partitions'][0]['leader']
                print(f"Topic '{topic_name}' leader for partition 0 is broker {leader_id}.")
                return True
            time.sleep(1)
        print(f"Warning: Topic '{topic_name}' leader not confirmed quickly.")
        return False # Could indicate an issue

    except TopicAlreadyExistsError:
        print(f"Topic '{topic_name}' already exists.")
        metadata = admin_client.describe_topics([topic_name])
        if metadata and metadata[0]['partitions'] and metadata[0]['partitions'][0]['leader'] != -1:
            leader_id = metadata[0]['partitions'][0]['leader']
            print(f"Topic '{topic_name}' leader for partition 0 is broker {leader_id}.")
        return True
    except KafkaError as e:
        print(f"Error creating topic '{topic_name}': {e}")
        return False

def delete_test_topic(admin_client, topic_name):
    try:
        print(f"Attempting to delete topic '{topic_name}'...")
        admin_client.delete_topics(topics=[topic_name])
        print(f"Topic '{topic_name}' deletion initiated.")
        # Deletion is asynchronous, actual removal might take time.
    except KafkaError as e:
        print(f"Error deleting topic '{topic_name}': {e}")

def produce_messages(bootstrap_servers, topic_name, num_messages=1000, message_size_kb=1):
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks='all', # Crucial for measuring replication latency via RemoteTimeMs
            retries=5,
            linger_ms=10 # Encourage some batching
        )
        print(f"Producing {num_messages} messages of approx {message_size_kb}KB to topic '{topic_name}' with acks='all'...")
        payload = b'X' * (message_size_kb * 1024)
        for i in range(num_messages):
            producer.send(topic_name, value=payload)
            if (i + 1) % (num_messages // 10 if num_messages >=10 else 1) == 0:
                print(f"  Sent {i+1}/{num_messages} messages...")
        producer.flush() # Block until all async messages are sent and acknowledged
        producer.close()
        print("Finished producing messages.")
        return True
    except NoBrokersAvailable:
        print(f"Error: No brokers available at {bootstrap_servers}. Is Kafka running and ports mapped correctly?")
        return False
    except KafkaError as e:
        print(f"Error producing messages: {e}")
        return False

def get_jmx_metric(jolokia_url, mbean, attribute=None):
    try:
        if attribute:
            url = f"{jolokia_url}/read/{mbean}/{attribute}"
        else:
            url = f"{jolokia_url}/read/{mbean}"
        
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        return data.get("value")
    except requests.exceptions.RequestException as e:
        # print(f"  JMX query failed for {jolokia_url} ({mbean}): {e}")
        return None
    except json.JSONDecodeError:
        # print(f"  JMX query failed for {jolokia_url} ({mbean}): Invalid JSON response")
        return None


def measure_replication_latency(num_brokers, base_kafka_port, test_topic_name, replication_factor, num_messages, message_size_kb):
    setup_kafka_broker_ports(num_brokers, base_kafka_port)
    jolokia_urls = get_jolokia_urls(num_brokers, base_kafka_port)
    bootstrap_servers = get_bootstrap_servers()

    admin_client = None
    try:
        print(f"Connecting to Kafka AdminClient at {bootstrap_servers}...")
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id='latency_tester_admin')
        print("AdminClient connected.")
    except Exception as e:
        print(f"Failed to connect Kafka AdminClient: {e}")
        print("Ensure Kafka brokers are running and accessible on the EXTERNAL listeners.")
        return

    if not create_test_topic(admin_client, test_topic_name, num_brokers, replication_factor):
        print("Failed to ensure test topic exists. Aborting.")
        if admin_client:
            admin_client.close()
        return

    # Allow some time for JMX metrics to reset or for previous counts to become less relevant
    # Or, even better, capture "before" and "after" counts if needed, but percentiles should be fresh
    print("Waiting a few seconds before producing messages for metrics to settle...")
    time.sleep(5)

    if not produce_messages(bootstrap_servers, test_topic_name, num_messages, message_size_kb):
        print("Message production failed. Aborting latency measurement.")
        if admin_client: # Try to clean up even if production failed
            delete_test_topic(admin_client, test_topic_name)
            admin_client.close()
        return
    
    print("Waiting a few seconds for replication metrics to be updated after producing...")
    time.sleep(5)

    leader_metrics_found = False
    print("\n--- Replication Latency (RemoteTimeMs for Produce Requests) ---")
    for i, jolokia_url in enumerate(jolokia_urls):
        broker_node_id = i * 2 # Assuming node IDs 0, 2, 4...
        print(f"\nQuerying Broker {broker_node_id} (Jolokia: {jolokia_url})...")
        metrics = get_jmx_metric(jolokia_url, MBEAN_PRODUCE_REMOTETIME)
        
        if metrics:
            count = metrics.get("Count", 0)
            one_min_rate = metrics.get("OneMinuteRate", 0.0)

            # Heuristic: if this broker processed produce requests recently, its Count will be higher
            # or OneMinuteRate will be non-zero.
            # For a fresh test, the leader of the single partition topic should have a count >= num_messages
            if count > 0 or one_min_rate > 0.01 : # Check if there's substantial activity
                leader_metrics_found = True
                print(f"  Broker {broker_node_id} shows Produce activity (Count: {count}, Rate: {one_min_rate:.2f}/s) - Likely a leader for some partitions.")
                print(f"  Latency Statistics (ms):")
                print(f"    Mean  : {metrics.get('Mean', 'N/A'):.3f}")
                print(f"    Min   : {metrics.get('Min', 'N/A'):.3f}")
                print(f"    Max   : {metrics.get('Max', 'N/A'):.3f}")
                print(f"    P50   : {metrics.get('50thPercentile', 'N/A'):.3f}")
                print(f"    P95   : {metrics.get('95thPercentile', 'N/A'):.3f}")
                print(f"    P99   : {metrics.get('99thPercentile', 'N/A'):.3f}")
                print(f"    P999  : {metrics.get('999thPercentile', 'N/A'):.3f}")
            else:
                print(f"  Broker {broker_node_id}: No significant Produce RemoteTimeMs activity observed (Count: {count}).")
        else:
            print(f"  Broker {broker_node_id}: Could not retrieve JMX metrics or MBean not found (check KAFKA_OPTS & Jolokia).")

    if not leader_metrics_found:
        print("\nNo leader broker activity found for Produce RemoteTimeMs.")
        print("Possible reasons:")
        print(" - Test topic leader could not be identified or did not process messages.")
        print(" - `acks='all'` was not effectively used by the producer.")
        print(" - Jolokia agent is not properly configured on one or more brokers.")
        print(" - MBean name is incorrect or not available.")

    # Cleanup
    if admin_client:
        delete_test_topic(admin_client, test_topic_name)
        admin_client.close()
        print("AdminClient closed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Measure Kafka Replication Latency.")
    parser.add_argument("-n", "--num_brokers", type=int, default=3, help="Number of Kafka brokers in the cluster.")
    parser.add_argument("-p", "--base_port", type=int, default=9090, help="Base port for Kafka services (e.g., 9090).")
    parser.add_argument("--topic", type=str, default="__latency_test_topic", help="Name for the temporary test topic.")
    parser.add_argument("--rf", type=int, default=3, help="Replication factor for the test topic.")
    parser.add_argument("--messages", type=int, default=1000, help="Number of messages to produce for the test.")
    parser.add_argument("--size_kb", type=int, default=1, help="Approximate size of each message in KB.")
    
    args = parser.parse_args()

    print("Starting Kafka Replication Latency Measurement...")
    print(f"Config: Brokers={args.num_brokers}, BasePort={args.base_port}, Topic={args.topic}, RF={args.rf}, Messages={args.messages}, SizeKB={args.size_kb}")
    
    # Ensure replication factor is not more than the number of brokers
    actual_rf = min(args.num_brokers, args.rf)
    if actual_rf != args.rf:
        print(f"Warning: Requested RF {args.rf} is > num_brokers {args.num_brokers}. Using RF={actual_rf}.")

    if args.num_brokers < 1:
        print("Error: Number of brokers must be at least 1.")
    elif actual_rf < 2 : # For RemoteTimeMs to be meaningful for replication, RF >= 2 (leader + follower)
        print(f"Warning: Replication Factor is {actual_rf}. `RemoteTimeMs` for Produce is most insightful with RF >= 2 and acks='all'.")
        print("         For RF=1, replication doesn't occur to other brokers.")


    measure_replication_latency(
        num_brokers=args.num_brokers,
        base_kafka_port=args.base_port,
        test_topic_name=args.topic,
        replication_factor_config=actual_rf,
        num_messages=args.messages,
        message_size_kb=args.size_kb
    )
    print("\nLatency measurement finished.")