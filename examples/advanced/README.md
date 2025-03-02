# Advanced Fogverse Examples

This directory demonstrates advanced Fogverse usage with multiple components managed by FogManager, deployment, and logging.

## Components

- **Frame Producer**: Captures video frames and sends them to Kafka
- **Frame Processor**: Processes frames (applies object detection)
- **Result Consumer**: Consumes processed results

## Manager Application

`manager_app.py` demonstrates the FogManager, which:

1. Sets up Kafka topics using the topic.yaml configuration
2. Deploys and manages components
3. Handles inter-component communication
4. Monitors component status

## Deployment

The setup demonstrates deploying components in a distributed environment:

- Components can run locally or in containers
- The FogManager coordinates component deployment
- Component status is tracked and managed

## Running the Example

1. Start Kafka and Zookeeper:

   ```bash
   docker-compose up -d kafka zookeeper
   ```

2. Run the manager application:

   ```bash
   python manager_app.py
   ```

3. Alternatively, deploy the full stack:
   ```bash
   docker-compose up
   ```

## Monitoring

The example includes logging and profiling to monitor performance:

- CSV logs track component performance
- Kafka topics for log aggregation
- Performance metrics collection
