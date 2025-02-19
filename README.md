# Fogverse - Distributed Stream Processing Framework

Fogverse is a Python framework for building distributed stream processing applications with Kafka. It provides high-level abstractions for producers, consumers, and cluster management while maintaining flexibility for customization.

## Features

- **High-Level Abstractions**: Easy-to-use interfaces for Kafka producers and consumers.
- **Async-First**: Built on `asyncio` for high-performance streaming.
- **Production-Ready**: Includes logging, error handling, and monitoring.
- **Modular Design**: Swap components via inheritance or configuration.
- **Cloud-Native**: Designed for Kubernetes and Docker deployments.

## Installation

```bash
pip install fogverse
```

## Quick Start

1. Start Kafka cluster:
   ```bash
   docker-compose -f examples/basic_usage/docker-compose.kafka.yml up
   ```
2. Run example producer:
   ```bash
   python examples/basic_usage/producer.py
   ```
3. Run example consumer:
   ```bash
   python examples/basic_usage/consumer.py
   ```

## Documentation

See the [examples](/examples/basic_usage) folder for detailed usage.
