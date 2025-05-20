import subprocess
import time
import yaml

from experiments.constants import MESSAGE_SIZES, NUM_RECORDS
from experiments.throughput import parse_consumer_perf_test, parse_prod_perf_test, run_consumer_test, run_producer_test
from experiments.utils.cleanup import cleanup
from experiments.utils.cluster_setup import setup_experiment_env
from experiments.utils.generate_payload import generate_payload
from fogverse.logger.fog import FogLogger

logger = FogLogger(f"comparison_{int(time.time())}")

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

import json
import os
from datetime import datetime

def main():
    # Dictionary to store results for each configuration and algorithm.
    results = {}

    logger.log_all("Starting experiments comparing Kafka topic partitioning algorithms using Prof. Claudio's default values and benchmark values.")

    # Iterate through both default and benchmark config files.
    for config_path in ["default-cluster-config.yaml", "benchmark-cluster-config.yaml"]:
        config = get_config(config_path)
        if not config:
            continue

        # Extract partitioning parameters from config file.
        params = config.get("partitioning_params", {})
        algorithms = params["algorithm"]

        # Normalize algorithm to a list if it's a single string.
        if not isinstance(algorithms, list):
            algorithms = [algorithms]

        # Use file name (without extension) as config key.
        config_name = os.path.splitext(os.path.basename(config_path))[0]
        results[config_name] = {}

        logger.log_all(f"Loading config from {config_path}...")
        logger.log_all("\n" + json.dumps(config, indent=2))
        logger.log_all(f"Config loaded successfully.")

        # Run experiments for each algorithm defined in the config.
        for algorithm in algorithms:
            logger.log_all(f"Running {algorithm} algorithm...")

            # Run external script to get number of partitions and brokers.
            partitions, brokers = run_pb_script(params, algorithm)
            logger.log_all(f"Retrieved results from {algorithm} algorithm showing {partitions} partitions and {brokers} brokers.")

            if not partitions or not brokers:
                logger.log_all(f"Skipping due to invalid partition/broker output.")
                continue

            results[config_name][algorithm] = {}

            # Iterate through different message sizes (in KB).
            for kb_size in MESSAGE_SIZES:
                # Scale number of records to send the same *total* data volume across tests.
                scaling_factor = max(MESSAGE_SIZES) / kb_size
                adjusted_num_records = int(NUM_RECORDS * scaling_factor)

                target_throughput_bps = params["T"]

                logger.log_all(
                    f"Testing config '{config_name}' with algorithm '{algorithm}' "
                    f"at {target_throughput_bps:.2e} bytes/s using message size {kb_size} KB "
                    f"across {partitions} partitions and {brokers} brokers."
                )

                # Set up Kafka environment with given broker/partition settings.
                setup_experiment_env(
                    logger,
                    num_partitions=partitions,
                    replication_factor=params["r"],
                    num_brokers=brokers
                )

                # Generate test payloads with fixed message size (min = max).
                generate_payload(logger, min_kb=kb_size, max_kb=kb_size)

                # Run producer performance test.
                producer_output = run_producer_test(
                    logger=logger,
                    record_size=(kb_size * 1_000),  # Convert KB to bytes.
                    throughput=target_throughput_bps,
                    log_output=True,
                    track_progress=False
                )
                producer_mbps, producer_latency = parse_prod_perf_test(producer_output)
                logger.log_all(
                    f"Production throughput is {producer_mbps:.4f} MB/s with latency of {producer_latency:.4f} ms."
                )

                # Run consumer performance test.
                consumer_output = run_consumer_test(
                    logger,
                    num_records=adjusted_num_records,
                    log_output=True
                )
                consumer_mbps, consumer_latency = parse_consumer_perf_test(consumer_output)
                logger.log_all(
                    f"Consumption throughput is {consumer_mbps:.4f} MB/s with latency of {consumer_latency:.4f} ms."
                )

                logger.log_all(f"Testing completed.")

                # Convert target throughput from bytes to MB/s.
                target_throughput_mbps = target_throughput_bps / 1_000_000

                # Store results in dictionary for later analysis/export.
                results[config_name][algorithm][f"{kb_size}KB"] = {
                    "partitions": int(partitions),
                    "brokers": int(brokers),
                    "storage_gb": (adjusted_num_records * kb_size) / 1_000_000,  # Total test volume in GB.
                    "target_throughput_mbps": target_throughput_mbps,
                    "producer": {
                        "throughput_mbps": producer_mbps,
                        "latency_ms": producer_latency,
                        "status": "success" if producer_mbps >= target_throughput_mbps else "failure"
                    },
                    "consumer": {
                        "throughput_mbps": consumer_mbps,
                        "latency_ms": consumer_latency,
                        "status": "success" if consumer_mbps >= target_throughput_mbps else "failure"
                    }
                }

                # Tear down environment to prepare for next iteration.
                cleanup(logger)

    # Generate timestamped filename to avoid overwriting old results.
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"perf_results_{timestamp}.json"

    # Write all collected test results to JSON file for later analysis.
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)

    # Log the location of the saved results.
    logger.log_all(f"Results saved to {output_file}.")

if __name__ == "__main__":
    main()
