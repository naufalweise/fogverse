import subprocess
import time
from datetime import datetime
import os
from zoneinfo import ZoneInfo


# --- Configuration ---
KAFKA_BROKERS = "my-cluster-kafka-bootstrap:9092"  # Replace with your Kafka broker list
TOPIC_NAME = "exp"
timezone = ZoneInfo("Asia/Jakarta")
START_TIME = datetime.now(tz=timezone)
OUTPUT_FOLDER = f"exp/output{START_TIME.strftime('%Y%m%d_%H%M%S')}"
NUM_RECORDS = int(os.environ.get("NUM_RECORDS", 10000))
NUM_CONSUMERS = int(os.environ.get("NUM_CONSUMERS", 1))
NUM_PRODUCERS = int(os.environ.get("NUM_PRODUCERS", 1))
THROUGHPUT_PER_PRODUCER = int(os.environ.get("THROUGHPUT_PER_PRODUCER", -1))
MESSAGE_SIZE_BYTES = int(os.environ.get("MESSAGE_SIZE_BYTES", 1024))
DURATION = int(os.environ.get("DURATION", 0)) # in minutes
PROD_SCALE_DELAY = int(os.environ.get("PROD_SCALE_DELAY", 0))

if DURATION:
    NUM_RECORDS = 9223372036854775807 # set to max

producer_pids = []
consumer_pids = []

# Create the output folder if it doesn't exist
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def run_command_background(command, stdout_file=None, stderr_file=None):
    """Runs a command in the background and redirects output to specified files."""
    stdout_fd = open(stdout_file, "w") if stdout_file else subprocess.PIPE
    stderr_fd = open(stderr_file, "w") if stderr_file else subprocess.PIPE
    process = subprocess.Popen(command, shell=True, stdout=stdout_fd, stderr=stderr_fd)
    if stdout_file:
        stdout_fd.close()
    if stderr_file:
        stderr_fd.close()
    return process.pid, process

def start_producers():
    print(f"Starting {NUM_PRODUCERS} producers...")
    for i in range(1, NUM_PRODUCERS + 1):
        print(f"Starting producer {i}...")
        stdout_file = os.path.join(OUTPUT_FOLDER, f"producer_{i}_stdout.log")
        stderr_file = os.path.join(OUTPUT_FOLDER, f"producer_{i}_stderr.log")
        command = (
            "date && kubectl exec kafka-client -n kafka -- kafka-producer-perf-test "
            f"--producer-props bootstrap.servers={KAFKA_BROKERS} "
            f"--topic {TOPIC_NAME} "
            f"--num-records {NUM_RECORDS} "
            f"--record-size {MESSAGE_SIZE_BYTES} "
            f"--throughput {THROUGHPUT_PER_PRODUCER} "
            "--print-metrics && date"
        )
        pid, process = run_command_background(command, stdout_file, stderr_file)
        producer_pids.append(pid)
        if PROD_SCALE_DELAY:
            print("Delaying...")
        time.sleep(PROD_SCALE_DELAY)
    print(f"All producers started with PIDs: {producer_pids}")

def start_consumers():
    print(f"Starting {NUM_CONSUMERS} consumers...")
    for i in range(1, NUM_CONSUMERS + 1):
        print(f"Starting consumer group consumer_{i}...")
        stdout_file = os.path.join(OUTPUT_FOLDER, f"consumer_{i}_stdout.log")
        stderr_file = os.path.join(OUTPUT_FOLDER, f"consumer_{i}_stderr.log")
        command = (
            "kubectl exec kafka-client -n kafka -- kafka-consumer-perf-test "
            f"--bootstrap-server {KAFKA_BROKERS} "
            f"--topic {TOPIC_NAME} "
            f"--messages {NUM_PRODUCERS * NUM_RECORDS} "
            f"--group test "
            "--print-metrics"
        )
        pid, process = run_command_background(command, stdout_file, stderr_file)
        consumer_pids.append(pid)
    print(f"All consumers started with PIDs: {consumer_pids}")

def wait_for_test():
    if DURATION:
        time.sleep(DURATION * 60)
        stop_processes()
    else:
        wait_for_prod()
    

def wait_for_prod():
    print("Waiting for producers to finish...")
    for pid in producer_pids:
        try:
            os.waitpid(pid, 0)  # Wait for each producer to terminate
            print(f"Producer PID {pid} finished.")
        except ChildProcessError:
            print(f"Producer PID {pid} already terminated or does not exist.")

    print("Killing all consumers...")
    for pid in consumer_pids:
        try:
            os.kill(pid, 2)  # SIGINT to gracefully terminate
            print(f"Sent SIGINT to consumer PID {pid}.")
        except ProcessLookupError:
            print(f"Consumer PID {pid} not found.")
        time.sleep(1) # Give some time for graceful termination

def stop_processes():
    print("Stopping producers...")
    for pid in producer_pids:
        try:
            os.kill(pid, 9)  # SIGKILL to forcefully terminate
            #os.kill(pid, 2)
            print(f"Sent SIGKILL to producer PID {pid}.")
        except ProcessLookupError:
            print(f"Producer PID {pid} not found.")

    print("Stopping consumers...")
    for pid in consumer_pids:
        try:
            #os.kill(pid, 9)  # SIGKILL to forcefully terminate
            os.kill(pid, 2)
            print(f"Sent SIGKILL to consumer PID {pid}.")
        except ProcessLookupError:
            print(f"Consumer PID {pid} not found.")

def collect_results():
    summary_file = os.path.join(OUTPUT_FOLDER, "performance_summary.txt")
    with open(summary_file, "w") as outfile:
        outfile.write(f"Start Test: {START_TIME.strftime('%Y-%m-%d %H:%M:%S')}\n")
        end_time = datetime.now(tz=timezone)
        outfile.write(f"End Test: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        outfile.write(f"Num Producers: {NUM_PRODUCERS}\n")
        outfile.write(f"Num Consumers: {NUM_CONSUMERS}\n")
        outfile.write(f"Message Size: {MESSAGE_SIZE_BYTES}\n")
        outfile.write(f"Prod Throughput: {THROUGHPUT_PER_PRODUCER}\n")
        outfile.write(f"Prod Scale Delay: {PROD_SCALE_DELAY}\n")
        if DURATION:
            outfile.write(f"Duration: {DURATION}")
        else:
            outfile.write(f"Num records: {NUM_RECORDS}\n")
        
        outfile.write(f"Output logs stored in: {OUTPUT_FOLDER}\n")
    print(f"Performance test summary saved to: {summary_file}")

if __name__ == "__main__":
    start_consumers()
    start_producers()
    wait_for_test()
    collect_results()
    print("Script finished.")