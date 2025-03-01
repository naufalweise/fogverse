import uuid
import aiokafka
import asyncio

from base import BaseProducer
from fogverse.runnable import Runnable
from logger import FogLogger
from utils.data import get_config

class KafkaProducer(BaseProducer, Runnable):
    """Kafka producer using aiokafka with configurable settings."""

    def __init__(self, loop=asyncio.get_event_loop()):
        self._loop = loop

        # Create consumer ID
        client_id = get_config("CLIENT_ID", default=str(uuid.uuid4()))

        # Create a logger instance.
        self._log = FogLogger(name=client_id)

        # Explicit list of topics to produce to.
        self.producer_topic = get_config("PRODUCER_TOPIC", default=[])

        # Kafka producer configuration.
        self.producer_conf = {
            "loop": self._loop,
            "bootstrap_servers": get_config("PRODUCER_SERVERS", default="localhost"),
            "client_id": client_id,
            **getattr(self, "producer_conf", {}),
        }

        # Initialize the Kafka producer.
        self.producer = aiokafka.AIOKafkaProducer(**self.producer_conf)

    async def start_producer(self):
        """ Starts the Kafka producer. """

        self._log_message(f"KAFKA PRODUCER START - TOPIC: {self.producer_topic}, CONFIG: {self.producer_conf}")
        await self.producer.start()

    async def _do_send(self, data, topic=None, key=None, headers=None):
        """
        Sends a message to the given Kafka topic.
        If no topic is specified, uses the default producer topic.
        """

        if not (topic := topic or self.producer_topic):
            raise ValueError("Topic should not be None.")

        return await self.producer.send(topic, value=data, key=key, headers=headers)

    async def close_producer(self):
        """Gracefully stops the Kafka producer."""

        await self.producer.stop()
        self._log_message("Producer has closed.")
