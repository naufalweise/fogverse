import asyncio
import json
import os
import socket
import sys
import traceback
import uuid

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from confluent_kafka.admin import AdminClient
from fogverse.constants import *
from fogverse.logger import FogLogger
from fogverse.utils.admin import setup_topics, read_topic_yaml
from pathlib import Path

# Default Kafka topic configuration settings.
_default_topic_config = {
    "retention.ms": 32768,  # Message retention duration.
    "segment.bytes": 1048576,  # Maximum segment size in bytes.
    "segment.ms": 8192,  # Time for segment rotation.
}

class FogManager:
    """Handles communication with Kafka and manages components in a distributed system."""

    def __init__(
        self,
        app_id=os.getenv("APP_ID") or str(uuid.uuid4()),
        components=[],
        to_deploy={},
        loop=asyncio.get_event_loop(),
        kafka_servers="localhost",
        topic_name="fogverse-commands",
        topic_config=_default_topic_config,
        partition_num=DEFAULT_NUM_PARTITIONS,
        log_dir="logs",
    ):
        # Ensures uniqueness across different instances of the application.
        self.app_id = app_id

        # Uniquely identifies this instance of the Manager.
        # Helps differentiate logs and messages when multiple managers exist.
        self.manager_id = f"Manager_{self.app_id}"

        # Ensures all async tasks (e.g., messaging, component execution) run in a single event loop.  
        # Using a shared loop prevents race conditions.  
        self.loop = loop

        # Each component is expected to be an object with its own execution logic,  
        # typically an asynchronous service that needs to be started and monitored.
        self.components = components

        # Handles structured logging and tracks Manager activity.
        self.logger = FogLogger(self.manager_id, level=FOGV_FILE, dirname=log_dir)

        # Determines which Kafka servers to connect to.
        self.manager_kafka_servers = (os.getenv("KAFKA_SERVERS") or kafka_servers)

        # Handles general Kafka administrative operations.
        self.admin = AdminClient({"bootstrap.servers": self.manager_kafka_servers})

        self.topic_name = topic_name
        self.topic_config = topic_config
        self.partition_num = partition_num

        # Constructs a Kafka configuration dictionary for both producers and consumers.
        # Integrates the shared event loop to coordinate async operations, sets the client ID as the host name for easy identification in Kafka logs,
        # and specifies the Kafka broker addresses for connectivity and message routing.
        self.kafka_config = {
            "loop": self.loop,
            "client_id": socket.gethostname(),
            "bootstrap_servers": self.manager_kafka_servers,
        }
        self.logger.std_log("INITIATING MANAGER: %s", self.kafka_config)

        self.producer = AIOKafkaProducer(**self.kafka_config)
        self.consumer = AIOKafkaConsumer(self.topic_name, **self.kafka_config)

        # Stores metadata for components that need to be deployed but haven"t started yet.
        self.to_deploy = to_deploy

        # Initializes an event that signals when components are ready.
        self.run_event = asyncio.Event()

        # Immediately signal readiness if there are no pending deployments.
        if not self.to_deploy:
            self.run_event.set()

    async def ensure_topics_exist(self):
        """
        Ensure Kafka topics exist by reading a yaml file, including the manager's topic,

        and creating/updating topics.
        """

        yaml_path = Path("topic.yaml")
        topic_data = read_topic_yaml(yaml_path, env_var_resolver={}) if yaml_path.is_file() else {}

        # Ensure the manager's topic is included.
        topics = topic_data.setdefault(self.manager_kafka_servers, {"admin": self.admin, "topics": []})["topics"]
        topics.append((self.topic_name, self.partition_num, self.topic_config))

        # Create or update topics.
        for data in topic_data.values():
            setup_topics(data["admin"], data["topics"])

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

        msg = json.dumps({"command": command, "from": self.manager_id, "message": message}).encode()
        return await self.producer.send(self.topic_name, msg, **kwargs)

    async def notify_running_status(self):
        """Notify that the application is running."""

        return await self.send_message(FOGV_STATUS_RUNNING, {"app_id": self.app_id})

    async def deploy_pending_components(self):
        """Deploy components that haven't started by sending deployment requests."""
        if self.run_event.is_set():
            return

        while True:
            self.logger.std_log("Deploying pending components: %s", self.to_deploy)
            pending = False

            for comp in self.to_deploy.values():
                if not comp.get("wait_to_start") or comp.get("status") == FOGV_STATUS_RUNNING:
                    continue

                if comp.get("status") is None:
                    pending = True # Mark that at least one component is still pending.

                msg = {
                    "image": comp["image"],
                    "app_id": comp["app_id"],
                    "env": comp["env"],
                }
                asyncio.create_task(self.send_message(FOGV_CMD_REQUEST_DEPLOY, msg))

            if not pending:
                break

            # Wait for 8 secs before retrying to prevent excessive requests.
            await asyncio.sleep(8)

        self.logger.std_log("Deployment requests complete.")
        self.run_event.set()

    async def send_shutdown(self):
        """Notify that the application is shutting down."""

        self.logger.std_log("Sending shutdown status")
        return await self.send_message(FOGV_STATUS_SHUTDOWN, {"app_id": self.app_id})

    async def handle_shutdown(self, message):
        """Handle shutdown request from Kafka."""

        if message["app_id"] == self.app_id:
            sys.exit(0)

    async def handle_running(self, message):
        """Update status of components when they start running."""

        for comp_data in self.to_deploy.values():
            if comp_data["app_id"] == message["app_id"]:
                comp_data["status"] = FOGV_STATUS_RUNNING

    async def handle_message(self, command, message):
        """Generic message handler (can be extended)."""

        self.logger.std_log("Handle general message: %s", message)

    async def receive_message(self):
        """Continuously consume and process Kafka messages from the message queue."""

        try:
            async for msg in self.consumer:  # Asynchronously iterate over received messages.
                if msg.value is None:
                    self.logger.std_log("Received empty message, skipping.")
                    continue

                data = json.loads(msg.value.decode())  # Decode the message content.
                command, message = data["command"], data["message"]  # Extract command and message.

                # Ignore messages sent by this Manager instance itself to prevent loops.
                if data["from"] == self.manager_id:
                    continue  

                self.logger.std_log("Received message: %s", data)

                # Dynamically resolve the appropriate handler based on the command.
                handler = getattr(self, f"handle_{command.lower()}", None)

                if callable(handler):
                    result = handler(message)  # Call the handler function.
                    if asyncio.iscoroutine(result):
                        await result  # Await if the handler is asynchronous.
                    continue  # Move to the next message.

                # If no specific handler exists, call the default handler.
                await self.handle_message(command, message)

        except Exception as e:
            self.logger.std_log("Exception in receiving messages.")
            self.logger.std_log(traceback.format_exc())  # Log full error traceback.
            raise e  # Re-raise the exception to propagate the error.

    async def run_components(self, components=None):
        """Start components once all dependencies have been resolved."""

        # Wait until the run_event is set, ensuring all dependencies are ready.
        if not self.run_event.is_set():
            self.logger.std_log("Waiting for dependencies to run.")
            await self.run_event.wait()  # Suspend execution until event is set.

        tasks = []

        try:
            # Determine which components to run (use provided list or default to self.components).
            tasks = [asyncio.ensure_future(c.run()) for c in (components or self.components)]

            # Notify that the manager and its components are running.
            await self.notify_running_status()

            # Run all components concurrently and wait until they complete.
            await asyncio.gather(*tasks)

        except Exception as e:
            self.logger.std_log("Exception in running components.")
            self.logger.std_log(traceback.format_exc()) # Log detailed traceback.

            # Cancel all running component tasks to ensure a clean shutdown.
            for t in tasks:
                t.cancel()

            raise e  # Propagate the error for higher-level handling.

    async def run(self):
        """Main execution method for the Manager, orchestrating the workflow."""

        # Ensure required Kafka topics exist before starting.
        await self.ensure_topics_exist()
        # Start the Kafka consumer and producer.
        await self.start()

        tasks = []

        try:
            # Start message receiving and component execution in parallel.
            tasks = [asyncio.ensure_future(self.receive_message()), 
                     asyncio.ensure_future(self.run_components())]

            self.logger.std_log("Manager %s is running.", self.manager_id)

            # Attempt to request deployment for required components.
            await self.deploy_pending_components()
            # Wait for all tasks to complete.
            await asyncio.gather(*tasks)

        except Exception as e:
            self.logger.std_log("Exception found while running Manager %s.", self.manager_id)
            self.logger.std_log(traceback.format_exc())  # Log detailed error traceback.

            # Cancel all tasks on error to prevent dangling processes.
            for t in tasks:
                t.cancel()

            raise e  # Re-raise the exception for handling at a higher level.

        finally:
            # Ensure Kafka services are properly stopped.
            await self.stop()
            self.logger.std_log("Manager has stopped.")
