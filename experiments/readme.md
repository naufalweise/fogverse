# Kafka Cluster Benchmarking

Automated benchmarking suite to compare BroMin and BroMax partitioning algorithms across different configurations.

## Prerequisites

- Python 3.8+
- Docker
- 8GB+ RAM
- ~20GB free disk space

## Running Tests

1. Generate benchmark configuration:
```bash
python experiments/benchmark_to_cluster_yaml.py
```
This runs initial tests to determine your system's actual capabilities.

2. Run comparison tests:
```bash
python experiments/comparison_test.py
```
This executes tests using both default and benchmark configurations.

3. Visualize results:
```bash
python experiments/visualize.py
```
Generates comparison plots in the current directory.

## Output Files

- `benchmark-cluster-config.yaml`: Generated cluster configuration based on your system
- `results/comparison_results_*.json`: Raw test results
- `producer_throughput_*.png`: Producer throughput comparison plots
- `producer_latency_*.png`: Producer latency comparison plots
- `consumer_throughput_*.png`: Consumer throughput comparison plots
- `consumer_fetch_ms_*.png`: Consumer fetch time comparison plots

## Important Notes

- Tests can take 30+ minutes to complete
- Ensure Docker daemon is running with sufficient resources
- Close other resource-intensive applications during testing
- All Kafka containers are automatically cleaned up after tests

## Troubleshooting

If tests fail:
1. Check Docker daemon is running: `docker ps`
2. Verify port range 9090-9099 is available
3. Clean up manually if needed: `docker rm -f $(docker ps -aq)`
4. Check logs

## Code Overview

Key Python files and their purposes:

### Main Scripts
- `benchmark_to_cluster_yaml.py`: Runs system benchmarks and generates optimized Kafka cluster configuration
- `comparison_test.py`: Executes comparative tests between default and benchmark configurations
- `visualize.py`: Generates performance comparison plots from test results
- `constants.py`: Centralizes configuration constants used across all scripts

### Core Test Modules
- `throughput.py`: Measures producer/consumer throughput using Kafka performance tools
- `replication_latency.py`: Measures data replication latency between brokers using JMX metrics
- `unavailability_time.py`: Measures broker failover and recovery times 
- `open_file_handles.py`: Determines system file descriptor limits

### Utils Package
- `utils/cleanup.py`: Removes Docker containers, volumes and temporary files
- `utils/cluster_setup.py`: Handles Kafka cluster deployment and configuration
- `utils/generate_docker_compose.py`: Creates Docker Compose files for Kafka clusters
- `utils/generate_jolokia_wrapper.py`: Creates wrapper script for JMX monitoring
- `utils/generate_payload.py`: Generates test data payloads
- `utils/run_cmd.py`: Utility for running shell commands with logging

### BroMin/BroMax Implementation
- `utils/bromin_bromax/algorithms.py`: Core implementation of partitioning algorithms
- `utils/bromin_bromax/get_pb.py`: CLI tool for calculating optimal