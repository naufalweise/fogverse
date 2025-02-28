import asyncio
import json
import logging
import os
import secrets
import socket
import sys
import traceback
import uuid

from ..admin import create_topics, read_topic_yaml
from ..fogverse_logging import FogVerseLogging
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from confluent_kafka import admin
from constants import *
from pathlib import Path

# Default Kafka topic configuration settings.
_default_topic_config = {
    'retention.ms': 32768,  # Message retention duration.
    'segment.bytes': 1048576,  # Maximum segment size in bytes.
    'segment.ms': 8192,  # Time for segment rotation.
}

# Default number of partitions for Kafka topics.
_default_partitions = 1

class Manager:
    """Handles communication with Kafka and manages components in a distributed system."""

    def __init__(
                self,
                components=None,
                loop=None,
                kafka_servers='localhost',
                app_id=None,
                topic='fogverse-commands',
                log_dir='logs',
                to_deploy=None,
                manager_topic_config=None,
                topic_str_format=_default_topic_config,
                manager_topic_partitions=_default_partitions
            ):

        # Ensures uniqueness across different instances of the application.
        self.app_id = os.getenv('APP_ID', app_id) or str(uuid.uuid4())

        # Uniquely identifies this instance of the Manager.
        # Helps differentiate logs and messages when multiple managers exist.
        self.manager_id = f'Manager_{self.app_id}'

        # Ensures all async tasks (e.g., messaging, component execution) run in a single event loop.  
        # Using a shared loop prevents race conditions.  
        self.loop = loop or asyncio.get_event_loop()

        # Each component is expected to be an object with its own execution logic,  
        # typically an asynchronous service that needs to be started and monitored.
        self.components = components or []

        # Logger instance for structured logging.
        # `FogVerseLogging` is assumed to be a custom logging utility.
        # Logs will be stored in the specified `log_dir`.
        self.logger = FogVerseLogging(self.manager_id, level=logging.FOGV_FILE, dirname=log_dir)

        # Determines which Kafka servers to connect to.
        # Checks multiple environment variables to support different deployment configurations.
        # Defaults to `kafka_servers` if no environment variable is set.
        self.manager_kafka_servers = (os.getenv('KAFKA_SERVERS') or
                                    os.getenv('CONSUMER_SERVERS') or
                                    os.getenv('PRODUCER_SERVERS') or
                                    kafka_servers)

        # Kafka AdminClient instance for managing topics.
        # Allows dynamic topic creation and configuration adjustments.
        self.admin = admin.AdminClient({'bootstrap.servers': self.manager_kafka_servers})

        # Kafka configuration used for both producers and consumers.
        # - `loop` ensures compatibility with async operations.
        # - `client_id` is set to the hostname for easy identification in Kafka logs.
        # - `bootstrap_servers` specifies which Kafka brokers to connect to.
        self.kafka_config = {
            'loop': self.loop,
            'client_id': socket.gethostname(),
            'bootstrap_servers': self.manager_kafka_servers,
        }

        # Log basic initialization details, useful for debugging.
        self.logger.std_log('INITIATING MANAGER: %s', self.kafka_config)

        # Kafka topic for receiving commands and sending responses.
        self.topic = topic

        # Kafka producer instance for sending messages.
        self.producer = AIOKafkaProducer(**self.kafka_config)

        # Kafka consumer instance for receiving messages.
        self.consumer = AIOKafkaConsumer(self.topic, **self.kafka_config)

        # Dictionary storing components that need to be deployed.
        # Keys: component types, Values: metadata (image, app_id, environment variables, etc.).
        # Defaults to an empty dictionary if no deployments are specified.
        self.to_deploy = to_deploy or {}

        # Event to signal when components are ready to run.
        # If `to_deploy` is empty, no dependencies exist, so we set it immediately.
        self.run_event = asyncio.Event()
        if not self.to_deploy:
            self.run_event.set()  # No dependencies, mark as ready.

        # Kafka topic configuration for managing partitions and other settings.
        # - `manager_topic_config`: Dictionary for topic-specific settings (e.g., retention time).
        # - `manager_topic_partitions`: Number of partitions for this Manager’s topic.
        # - `topic_str_format`: Formatting rules for dynamically created topics.
        self.manager_topic_config = manager_topic_config or {}
        self.manager_topic_partitions = manager_topic_partitions
        self.topic_str_format = topic_str_format

    async def check_topics(self):
        """Ensure required Kafka topics exist."""

        yaml_path = Path('topic.yaml')  # Path to topic configuration file.
        topic_data = {}

        if yaml_path.is_file():
            topic_data = read_topic_yaml(yaml_path, str_format=self.topic_str_format)

        # Ensure the manager's topic is included.
        topic_data.setdefault(self.manager_kafka_servers, {'admin': self.admin, 'topics': []})
        topic_data[self.manager_kafka_servers]['topics'].append(
            (self.topic, self.manager_topic_partitions, self.manager_topic_config)
        )

        # Create or update topics.
        for data in topic_data.values():
            create_topics(data['admin'], data['topics'])

    async def start(self):
        """Start Kafka producer and consumer."""

        await self.consumer.start()
        await self.producer.start()

    async def stop(self):
        """Stop Kafka producer and consumer."""

        await self.consumer.stop()
        await self.producer.stop()

    async def send_message(self, command, message, **kwargs):
        """Send a message to Kafka with a specific command."""

        msg = json.dumps({'command': command, 'from': self.manager_id, 'message': message}).encode()
        return await self.producer.send(self.topic, msg, **kwargs)

    async def send_running_message(self):
        """Send a running status message."""

        return await self.send_message(FOGV_STATUS_RUNNING, {'app_id': self.app_id})

    async def send_request_component(self):
        """Request deployment for components that haven't started yet."""

        # Keep looping until the run_event is set, indicating readiness to proceed.
        while not self.run_event.is_set():
            self.logger.std_log('Sending request component')
            self.logger.std_log('to_deploy: %s', self.to_deploy)

            pending_comp = False  # Flag to check if any components are still pending.

            # Iterate over components that need to be deployed.
            for comp_type, comp_data in self.to_deploy.items():
                # Skip components that are not marked as needing to wait before starting.
                if not comp_data.get('wait_to_start', False):
                    continue

                # Skip components that are already running.
                if comp_data.get('status') == FOGV_STATUS_RUNNING:
                    continue

                # If status is None, it means the component hasn't been deployed yet.
                if comp_data.get('status') is None:
                    pending_comp = True  # Mark that at least one component is still pending.

                # Construct the deployment request message.
                msg = {
                    'image': comp_data['image'],  # Docker image or executable reference.
                    'app_id': comp_data['app_id'],  # Unique identifier for the application.
                    'env': comp_data['env'],  # Environment variables for the component.
                }

                # Send the deployment request asynchronously.
                asyncio.ensure_future(self.send_message(FOGV_CMD_REQUEST_DEPLOY, msg))

            # If there are no pending components, break the loop.
            if not pending_comp:
                break  # Exit loop if all components have started.

            # Wait for 10 seconds before retrying to prevent excessive requests.
            await asyncio.sleep(10)

        self.logger.std_log('Leaving component request sending procedure')
        self.run_event.set()  # Set run_event to indicate readiness.

    async def send_shutdown(self):
        """Send a shutdown status message."""

        self.logger.std_log('Sending shutdown status')
        return await self.send_message(FOGV_STATUS_SHUTDOWN, {'app_id': self.app_id})

    async def handle_shutdown(self, message):
        """Handle shutdown request from Kafka."""

        if message['app_id'] == self.app_id:
            sys.exit(0)

    async def handle_running(self, message):
        """Update status of components when they start running."""

        for comp_data in self.to_deploy.values():
            if comp_data['app_id'] == message['app_id']:
                comp_data['status'] = FOGV_STATUS_RUNNING

    async def handle_message(self, command, message):
        """Generic message handler (can be extended)."""

        self.logger.std_log('Handle general message')

    async def receive_message(self):
        """Continuously consume and process Kafka messages from the message queue."""

        try:
            async for msg in self.consumer:  # Asynchronously iterate over received messages.
                data = json.loads(msg.value.decode())  # Decode the message content.

                command, message = data['command'], data['message']  # Extract command and message.

                # Ignore messages sent by this Manager instance itself to prevent loops.
                if data['from'] == self.manager_id:
                    continue  

                self.logger.std_log('Received message: %s', data)

                # Dynamically resolve the appropriate handler based on the command.
                handler = getattr(self, f'handle_{command.lower()}', None)

                if callable(handler):
                    result = handler(message)  # Call the handler function.
                    if asyncio.iscoroutine(result):
                        await result  # Await if the handler is asynchronous.
                    continue  # Move to the next message.

                # If no specific handler exists, call the default handler.
                await self.handle_message(command, message)

        except Exception as e:
            self.logger.std_log('Exception in receive_message')
            self.logger.std_log(traceback.format_exc())  # Log full error traceback.
            raise e  # Re-raise the exception to propagate the error.

    async def run_components(self, components=None):
        """Start components once all dependencies have been resolved."""

        # Wait until the run_event is set, ensuring all dependencies are ready.
        if not self.run_event.is_set():
            self.logger.std_log('Waiting for dependencies to run')
            await self.run_event.wait()  # Suspend execution until event is set.

        try:
            # Determine which components to run (use provided list or default to self.components).
            tasks = [asyncio.ensure_future(c.run()) for c in (components or self.components)]

            # Notify that the manager and its components are running.
            await self.send_running_message()

            # Run all components concurrently and wait until they complete.
            await asyncio.gather(*tasks)

        except Exception as e:
            self.logger.std_log('Exception in run_components')
            self.logger.std_log(traceback.format_exc())  # Log detailed traceback.

            # Cancel all running component tasks to ensure a clean shutdown.
            for t in tasks:
                t.cancel()

            raise e  # Propagate the error for higher-level handling.

    async def run(self):
        """Main execution method for the Manager, orchestrating the workflow."""

        # Ensure required Kafka topics exist before starting.
        await self.check_topics()

        # Start the Kafka consumer and producer.
        await self.start()

        try:
            # Start message receiving and component execution in parallel.
            tasks = [asyncio.ensure_future(self.receive_message()), 
                     asyncio.ensure_future(self.run_components())]

            self.logger.std_log('Manager %s is running', self.manager_id)

            # Attempt to request deployment for required components.
            await self.send_request_component()

            # Wait for all tasks to complete.
            await asyncio.gather(*tasks)

        except Exception as e:
            self.logger.std_log('Exception in run')
            self.logger.std_log(traceback.format_exc())  # Log detailed error traceback.

            # Cancel all tasks on error to prevent dangling processes.
            for t in tasks:
                t.cancel()

            raise e  # Re-raise the exception for handling at a higher level.

        finally:
            # Ensure Kafka services are properly stopped.
            await self.stop()
            self.logger.std_log('Manager has stopped')
