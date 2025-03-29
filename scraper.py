import os
import time
import json
import re
import subprocess

from kafka import KafkaAdminClient, KafkaProducer
from kafka.errors import UnknownTopicOrPartitionError, NoBrokersAvailable

def get_cpu_perc(container_name):
    try:
        # Run docker stats in no-stream mode, output in JSON.
        result = subprocess.run(
            ["docker", "stats", container_name, "--no-stream", "--format", "{{json .}}"],
            capture_output=True,
            text=True,
            check=True
        )
        stats = json.loads(result.stdout.strip())

        # Remove '%' and whitespace from the CPU usage value and convert it to a float.
        cpu = float(re.sub(r"[%\s]+", "", stats.get("CPU", "0%")))
        return cpu
    except Exception as e:
        print("ERROR:", e)
        return None

def benchmark_production_throughput_per_partition():
    container = "kafka-broker-1"
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:29091")
    print("Kafka Admin Client created.")
    try:
        topics = admin_client.list_topics()
        if "test-topic" in topics:
            print("Deleting topic 'test-topic'...")
            admin_client.delete_topics(["test-topic"])
        else:
            print("Topic 'test-topic' does not exist.")
    except UnknownTopicOrPartitionError:
        print("Topic does not exist.")
    except Exception as e:
        print(f"ERROR: {e}")
    finally:
        admin_client.close()


    # cpu_perc = get_cpu_perc(container)

    # if cpu_perc is not None:
    #     print(f"Container '{container}' CPU Usage: {cpu_perc:.2f}%")
    # print("\n")
    # time.sleep(1)

def setup_cluster():
    pass

# def run_command(command):
#     process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#     stdout, stderr = process.communicate()

#     if stdout:
#         print(stdout.strip())
#     if stderr:
#         print(f"ERROR: {stderr.strip()}")

#     return stdout.strip()

# def delete_kafka_topic_if_exists():
#     kafka_dir = os.getenv("KAFKA_DIR")
#     remote_host = os.getenv("REMOTEHOST", "localhost")
#     bootstrap_server = f"{remote_host}:19091"
#     kafka_topics_cmd = f"{kafka_dir}/bin/kafka-topics.sh"

#     check_cmd = "./kafka/bin/kafka-topics.sh --list --topic test-topic --bootstrap-server localhost:29091"
#     existing_topic = run_command(check_cmd)

#     if existing_topic:
#         print("Deleting topic 'test-topic'...")
#         delete_cmd = f"{kafka_topics_cmd} --delete --topic test-topic --bootstrap-server {bootstrap_server}"
#         run_command(delete_cmd)
#     else:
#         print("Topic 'test-topic' does not exist.")

if __name__ == "__main__":
    try:
        # Stop and remove existing containers and volumes.
        subprocess.run(["docker", "compose", "down", "-v"], check=True)

        # Determine the appropriate Python command.
        python_venv_dir = os.getenv("PYTHON_VENV_DIR")
        generate_compose_cmd = os.path.join(python_venv_dir, "Scripts", "python") if python_venv_dir and os.path.exists(python_venv_dir) else "python"

        # Generate the docker-compose configuration, then start the Docker services.
        subprocess.run([generate_compose_cmd, "generate_compose.py"], check=True)
        subprocess.run(["docker", "compose", "up", "-d"], check=True)

        while True:
            try:
                admin_client = KafkaAdminClient(bootstrap_servers="localhost:29091")
                admin_client.close()
                print("Kafka is ready!")
                break
            except NoBrokersAvailable:
                print("Waiting for Kafka...")
                time.sleep(1)  # Retry every second

        benchmark_production_throughput_per_partition()
    except Exception as e:
        print("ERROR:", e)
    finally:
        # Clean up Docker containers and volumes after testing.
        subprocess.run(["docker", "compose", "down", "-v"], check=True)
        print("Docker containers stopped and removed.")

def produce_test_messages(topic, num_records=1000, record_size=100, bootstrap_servers="localhost:29091"):
    """Sends test messages to a Kafka topic to benchmark throughput."""
    producer = KafkaProducer(
        
        bootstrap_servers=bootstrap_servers,
        linger_ms=0  # Ensures immediate sending, similar to `--throughput -1`
    )

    message = b"x" * record_size  # Create a fixed-size message
    start_time = time.time()

    for _ in range(num_records):
        producer.send(topic, message)

    producer.flush()  # Ensure all messages are sent
    elapsed_time = time.time() - start_time
    print(f"Sent {num_records} messages in {elapsed_time:.2f} seconds")

# ./kafka/bin/kafka-producer-perf-test.sh --topic test-topic --num-records 1000 --record-size 100 --throughput -1 --producer-props bootstrap.servers=localhost:29091
# 1000 records sent, 2247.2 records/sec (0.21 MB/sec), 26.79 ms avg latency, 411.00 ms max latency, 26 ms 50th, 46 ms 95th, 50 ms 99th, 411 ms 99.9th.