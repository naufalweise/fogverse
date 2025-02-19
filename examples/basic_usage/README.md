# Basic Usage Example

This example demonstrates how to use Fogverse to create a simple Kafka producer and consumer.

## Setup

1. Start Kafka cluster:

   ```bash
   docker-compose -f docker-compose.kafka.yml up
   ```

2. Run the producer:
   ```bash
   python producer.py
   ```
3. Run the consumer:
   ```bash
   python consumer.py
   ```
