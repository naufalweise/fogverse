import re
import subprocess
import time
import yaml

from experiments.constants import BOOTSTRAP_SERVER, NUM_RECORDS, TARGET_THROUGHPUT, TOPIC_NAME
from experiments.utils.run_cmd import run_cmd
from fogverse.logger.fog import FogLogger

logger = FogLogger(f"throughput_{int(time.time())}")

def run_producer_test(logger=logger, bootstrap_server=BOOTSTRAP_SERVER, topic_name=TOPIC_NAME, num_records=NUM_RECORDS):
    # Run the Kafka producer performance test using payload.txt and track progress.
    cmd = (
        "kafka/bin/kafka-producer-perf-test.sh "
        f"--producer-props acks=all bootstrap.servers={bootstrap_server} "
        f"--topic {topic_name} "
        f"--num-records {num_records} "
        "--payload-file payload.txt "
        "--throughput -1 "
    )
    logger.log_all(f"Running producer with {num_records} records...")

    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    output = []

    running_total = 0
    for line in process.stdout:
        line = line.strip()
        output.append(line)

        match = re.match(r"(\d+)\s+records sent", line)
        if match:
            this_count = int(match.group(1))

            if this_count == num_records:
                logger.log_all("100% progress completed.")
            else:
                running_total += this_count
                percent = int((running_total / num_records) * 100)
                logger.log_all(f"{percent}% progress completed.")

    process.wait()
    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, cmd)

    return "\n".join(output)

def run_consumer_test(bootstrap_server=BOOTSTRAP_SERVER, topic_name=TOPIC_NAME, num_records=NUM_RECORDS):
    # Run the Kafka consumer performance test.
    cmd = (
        "kafka/bin/kafka-consumer-perf-test.sh "
        f"--bootstrap-server {bootstrap_server} "
        f"--topic {topic_name} "
        f"--messages {num_records} "
        "--show-detailed-stats"
    )
    logger.log_all(f"Running consumer with {num_records} records...")
    result = run_cmd(cmd, capture_output=True)
    return result.stdout

def parse_prod_perf_test(output):
    # For producer, take the last line with MB/sec in parentheses.
    lines = output.strip().splitlines()
    for line in reversed(lines):
        match = re.search(r'\((\d+\.\d+) MB/sec\)', line)
        if match:
            return float(match.group(1))
    return 0.0

def parse_consumer_perf_test(output):
    # For consumer, average MB.sec column, skipping the first row.
    mb_sec_values = []
    lines = output.strip().splitlines()
    for line in lines:
        if line.startswith('time,'):
            continue  # Skip header.
        fields = line.split(',')
        try:
            mb_sec = float(fields[3])  # MB.sec is the 4th column.
            mb_sec_values.append(mb_sec)
        except (IndexError, ValueError):
            continue
    if mb_sec_values:
        return sum(mb_sec_values) / len(mb_sec_values)
    else:
        return 0.0

def get_config(path):
    try:
        with open(path, "r") as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        print(f"YAML file not found: {path}")
    except yaml.YAMLError as e:
        print(f"Parsing error in {path}: {e}")

def run_pb_script(params, algorithm):
    script_args = ['python', './experiments/utils/bromin_bromax/get_pb.py']
    script_args.extend(['--algorithm', algorithm])

    for param, value in params.items():
        if param == 'algorithm':
            continue
        script_args.append(f'--{param}')
        script_args.append(str(value))

    result = subprocess.run(script_args, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"[{algorithm}] ERROR: {result.stderr.strip()}")
        return None, None

    try:
        num_partitions, num_brokers = result.stdout.strip().split()
        return num_partitions, num_brokers
    except ValueError:
        print(f"[{algorithm}] Unexpected output: {result.stdout.strip()}")
        return None, None

def main():
    for config_path in ["default-cluster-config.yaml", "benchmark-cluster-config.yaml"]:
        config = get_config(config_path)
        if not config:
            continue

        params = config.get("partitioning_params", {})
        algorithms = params.get("algorithm", [])
        if not isinstance(algorithms, list):
            algorithms = [algorithms]

        for algo in algorithms:
            for mbps in TARGET_THROUGHPUT:
                throughput_bps = mbps * 1e6
                params["T"] = throughput_bps

                partitions, brokers = run_pb_script(params, algo)
                if partitions and brokers:
                    print(f"[{config_path}] {algo} @ {mbps} MB/s: Partitions={partitions}, Brokers={brokers}")

if __name__ == "__main__":
    main()
