import re
import subprocess
import time
import yaml

from experiments.constants import BOOTSTRAP_SERVER, MESSAGE_SIZES, NUM_RECORDS, TOPIC_NAME
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
    results = {}
    logger.log_all("Starting experiments comparing Kafka topic partitioning algorithms using Prof. Claudio's default values and benchmark values.")

    for config_path in ["default-cluster-config.yaml", "benchmark-cluster-config.yaml"]:
        config = get_config(config_path)
        if not config:
            continue

        params = config.get("partitioning_params", {})
        algorithms = params.get("algorithm", [])
        if not isinstance(algorithms, list):
            algorithms = [algorithms]

        # Store result for this config.
        config_name = os.path.splitext(os.path.basename(config_path))[0]
        results[config_name] = {}

        logger.log_all(f"Loading config from {config_path}...")
        logger.log_all(json.dumps(config, indent=2))
        logger.log_all(f"Config loaded successfully.")

        for algorithm in algorithms:
            logger.log_all(f"Running {algorithm} algorithm...")
            partitions, brokers = run_pb_script(params, algorithm)
            logger.log_all(f"Retrieved results from {algorithm} algorithm showing {partitions} partitions and {brokers} brokers.")

            if not partitions or not brokers:
                logger.log_all(f"Skipping due to invalid partition/broker output.")
                continue

            results[config_name][algorithm] = {}

            for kb_size in MESSAGE_SIZES:
                message_bytes = kb_size * 1_000
                throughput_target_bps = params["T"]

                logger.log_all(f"Testing at {throughput_target_bps:.2e} bytes/s with message size {kb_size} KB across {partitions} partitions and {brokers} brokers.")

                setup_experiment_env(logger, num_partitions=partitions, num_brokers=brokers)
                generate_payload(logger, min_kb=kb_size, max_kb=kb_size)

                # Run producer and consumer.
                producer_output = run_producer_test(
                    logger=logger,
                    record_size=message_bytes,
                    throughput=throughput_target_bps,
                    log_output=True,
                    track_progress=False
                )
                producer_mbps, producer_latency = parse_prod_perf_test(producer_output)
                logger.log_all(f"Production throughput is {producer_mbps:.4f} MB/s with latency of {producer_latency:.4f} ms.")

                consumer_output = run_consumer_test(log_output=True)
                consumer_mbps, consumer_latency = parse_consumer_perf_test(consumer_output)
                logger.log_all(f"Consumption throughput is {consumer_mbps:.4f} MB/s with latency of {consumer_latency:.4f} ms.")

                logger.log_all(f"Testing completed.")
                # Store in results dict.
                results[config_name][algorithm][f"{kb_size}KB"] = {
                    "partitions": int(partitions),
                    "brokers": int(brokers),
                    "record_size_bytes": message_bytes,
                    "throughput_target_bps": throughput_target_bps,
                    "producer": {
                        "throughput_MBps": producer_mbps,
                        "latency_ms": producer_latency
                    },
                    "consumer": {
                        "throughput_MBps": consumer_mbps,
                        "latency_ms": consumer_latency
                    }
                }

                cleanup(logger)

    # Save results.
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"perf_results_{timestamp}.json"
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)

    logger.log_all(f"Results saved to {output_file}.")

if __name__ == "__main__":
    main()
