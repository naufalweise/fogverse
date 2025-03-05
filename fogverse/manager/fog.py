import asyncio
import json
import socket
import sys
import time
import traceback

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from confluent_kafka.admin import AdminClient
from fogverse.constants import *
from fogverse.logger import FogLogger

class FogManager:
    """Handles communication with Kafka and manages components in a distributed system."""

    def __init__(self, name=f"Manager_{int(time.time())}", log_dir="logs", components=[], to_deploy={},
                 kafka_server="localhost", topic_name="fogverse-commands",
                 topic_config={
                        "retention.ms": 32768,  # Message retention duration.
                        "segment.bytes": 1048576,  # Maximum segment size in bytes.
                        "segment.ms": 8192,  # Time for segment rotation.
                    },
                 num_partitions=DEFAULT_NUM_PARTITIONS):
        super().__init__()

        self.name = name
        self.components = components
        self.to_deploy = to_deploy

        self.logger = FogLogger(self.name, dirname=log_dir)
        self.kafka_server = kafka_server
        self.admin = AdminClient({"bootstrap.servers": kafka_server})

        self.topic_name = topic_name
        self.topic_config = topic_config
        self.num_partitions = num_partitions

        self.kafka_config = {
            "client_id": socket.gethostname(),
            "bootstrap_servers": kafka_server,
        }
        self.logger.std_log("INITIATING MANAGER: %s", self.kafka_config)

        self.producer = AIOKafkaProducer(**self.kafka_config)
        self.consumer = AIOKafkaConsumer(topic_name, **self.kafka_config)

        # Event to signal readiness once deployments are complete.
        self.run_event = asyncio.Event()

        if not self.to_deploy: self.run_event.set()

    async def start(self):
        await self.consumer.start()
        await self.producer.start()

    async def stop(self):
        await self.consumer.stop()
        await self.producer.stop()

    async def send_message(self, command, message, **kwargs):
        """Send a message to Kafka with a specific command."""

        msg = json.dumps({"command": command, "from": self.name, "message": message}).encode()
        return await self.producer.send(self.topic_name, msg, **kwargs)

    async def notify_running_status(self):
        """Notify that the application is running."""

        return await self.send_message(FOGV_STATUS_RUNNING, {"name": self.name})

    async def deploy_pending_components(self):
        """Deploy components that haven't started by sending deployment requests."""

        if self.run_event.is_set(): return

        while True:
            self.logger.std_log("Deploying pending components: %s", self.to_deploy)
            pending = False

            for comp in self.to_deploy.values():
                if not comp.get("wait_to_start") or comp.get("status") == FOGV_STATUS_RUNNING:
                    continue

                if comp.get("status") is None:
                    pending = True  # Mark that at least one component is still pending.

                msg = {
                    "name": comp["name"],
                    "image": comp["image"],
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
        return await self.send_message(FOGV_STATUS_SHUTDOWN, {"name": self.name})

    async def handle_shutdown(self, message):
        """Handle shutdown request from Kafka."""

        if message["name"] == self.name:
            sys.exit(0)

    async def handle_running(self, message):
        """Update status of components when they start running."""

        for comp_data in self.to_deploy.values():
            if comp_data["name"] == message["name"]:
                comp_data["status"] = FOGV_STATUS_RUNNING

    async def handle_message(self, command, message):
        """Generic message handler (can be extended)."""

        pass

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
                if data["from"] == self.name:
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

            self.logger.std_log("Manager named %s is running.", self.name)

            # Attempt to request deployment for required components.
            await self.deploy_pending_components()
            # Wait for all tasks to complete.
            await asyncio.gather(*tasks)

        except Exception as e:
            self.logger.std_log("Exception found while running Manager %s.", self.name)
            self.logger.std_log(traceback.format_exc())  # Log detailed error traceback.

            # Cancel all tasks on error to prevent dangling processes.
            for t in tasks:
                t.cancel()

            raise e  # Re-raise the exception for handling at a higher level.

        finally:
            # Ensure Kafka services are properly stopped.
            await self.stop()
            self.logger.std_log("Manager has stopped.")
