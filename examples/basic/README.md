# Basic Fogverse Examples

This directory contains simple examples demonstrating basic fogverse functionality.

## Producer-Consumer Example

`producer_consumer.py` shows a basic Kafka producer and consumer setup:

1. The producer sends messages to a Kafka topic.
2. The consumer receives and processes these messages.
3. The example uses fogverse's built-in components for simplified messaging.

Run with:

```bash
python producer_consumer.py
```

## Image Processor Example

`image_processor.py` demonstrates processing video frames:

1. Captures frames from a camera or video file
2. Processes each frame
3. Sends the processed frames to a Kafka topic

Run with:

```bash
python image_processor.py
```

## Docker Setup

Use the provided docker-compose.yml to run Kafka and Zookeeper:

```bash
docker-compose up -d
```
