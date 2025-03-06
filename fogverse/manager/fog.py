import asyncio
import docker
import json
import socket
import sys
import time
import traceback

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fogverse.constants import *
from fogverse.consumer.kafka import KafkaConsumer
from fogverse.logger import FogLogger
from fogverse.producer.kafka import KafkaProducer
from fogverse.utils.admin import configure_topics
from fogverse.utils.data import get_config

class FogManager:
    """
    Handles communication with Kafka and manages components in a distributed system.
    Set the 'server' and 'topic' parameters, or define the MANAGER_SERVER and MANAGER_TOPIC environment variables.
    """

    def __init__(self, name=f"Manager_{int(time.time())}", log_dir="logs", server=None, topic=None, yaml="topics.yaml", components=[], to_deploy={}):
        super().__init__()

        self.name = name
        self.components = components
        self.to_deploy = to_deploy

        self.logger = FogLogger(self.name, dirname=log_dir)

        self.server = server or get_config('MANAGER_SERVER', default="localhost")
        self.topic = topic or get_config('MANAGER_TOPIC', default="manager-topic")
        self.yaml = yaml

        self.producer = KafkaProducer(f"{self.name}_producer", self.server, self.topic)
        self.consumer = KafkaConsumer(socket.gethostname(), f"{self.name}_consumer", self.server, self.topic)

        self.docker_client = docker.from_env()
        self.deployed_components = {}

        self.command_handlers = {
            FOGV_STATUS_RUNNING: self.handle_running,
            FOGV_STATUS_SHUTDOWN: self.handle_shutdown,
        }

        # Event to signal readiness once deployments are complete.
        self.run_event = asyncio.Event()
        if not self.to_deploy: self.run_event.set()

    async def start(self):
        await self.consumer.start_consumer()
        await self.producer.start_producer()

    async def close(self):
        await self.consumer.close_consumer()
        await self.producer.close_producer()


    async def notify_running_status(self):
        """Notify that the application is running."""

        return await self.send_message(FOGV_STATUS_RUNNING, {"name": self.name})

    async def send_message(self, command, message, **kwargs):
        """Send a message to Kafka with a specific command."""

        # msg = json.dumps({"command": command, "from": self.name, "message": message}).encode()
        return await self.producer.send(topic=self.topic, data=message, **kwargs)

    async def init_deploy(self):
        """Deploy components that haven't started by sending deployment requests."""

        if self.run_event.is_set(): return

        # for name, config in self.to_deploy.items():
        #     self.logger.std_log(f"DEPLOYING: {name}")

        #     self.docker_client.containers.run(
        #         name=name,
        #         image=config["image"],
        #         environment=config["env"],
        #         detach=True,
        #     )
        #     self.to_deploy[name]["status"] = FOGV_STATUS_RUNNING

        # self.logger.std_log("Deployment requests complete.")
        # self.run_event.set()
        
        message = "hello"
        x = await self.send_message("INFORM", message.encode())
        print()

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

        for comp in self.to_deploy.values():
            if comp["name"] == message["name"]:
                comp["status"] = FOGV_STATUS_RUNNING

    async def receive_message(self):
        """Continuously consume and process Kafka messages"""

        try:
            while True:
                raw = await self.consumer.receive()
                print(raw)
                if raw is None: continue  # Nothing to process.

                data = json.loads(raw.value.decode())
                command = data["command"]
                message = data["message"]

                handler = self.command_handlers.get(command)
                if handler:
                    await handler(message) if asyncio.iscoroutinefunction(handler) else handler(message)
                else:
                    self.logger.std_log(f"COMMAND NOT FOUND: {command}")

        except asyncio.CancelledError:
            self.logger.std_log("Message consumption cancelled")
            raise

    async def run_components(self, components=None):
        """Start components once all dependencies have been resolved."""

        # # Wait until the run_event is set, ensuring all dependencies are ready.
        # if not self.run_event.is_set():
        #     self.logger.std_log("Waiting for dependencies to run.")
        #     await self.run_event.wait()  # Suspend execution until event is set.

        tasks = []

        try:
            # Determine which components to run (use provided list or default to self.components).
            tasks = [asyncio.ensure_future(c.run()) for c in (components or self.components)]

            # Notify that the manager and its components are running.
            # await self.notify_running_status()

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

        configure_topics(self.yaml, create=True)
        await self.start()

        tasks = []

        try:
            tasks = [asyncio.ensure_future(self.receive_message()),
                     asyncio.ensure_future(self.run_components())]

            self.logger.std_log("Manager named %s is running.", self.name)

            await self.init_deploy()
            await asyncio.gather(*tasks)

        except Exception as e:
            self.logger.std_log("Exception found while running Manager %s.", self.name)
            self.logger.std_log(traceback.format_exc())

            for t in tasks:
                t.cancel()

            raise e

        finally:
            await self.close()
            self.logger.std_log("Manager has stopped.")
