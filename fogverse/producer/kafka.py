import asyncio
import socket

from aiokafka import AIOKafkaProducer
from utils.data import get_config
from ..logger import FogVerseLogging
from .base import BaseProducer

class KafkaProducer(BaseProducer):
    """ Kafka producer using aiokafka with configurable settings. """

    def __init__(self, loop=None):
        self._loop = loop or asyncio.get_event_loop()

        # Get producer configuration from environment.
        self.producer_topic = get_config("PRODUCER_TOPIC", self)
        self.producer_conf = {
            "loop": self._loop,
            "bootstrap_servers": get_config("PRODUCER_SERVERS", self),
            "client_id": get_config("CLIENT_ID", self, socket.gethostname()),
            **getattr(self, "producer_conf", {}),
        }

        # Initialize the Kafka producer.
        self.producer = AIOKafkaProducer(**self.producer_conf)

    async def start_producer(self):
        """ Starts the Kafka producer. """

        self._log_message(f"Starting producer - Config: {self.producer_conf}, Topic: {self.producer_topic}")
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
        """ Gracefully stops the Kafka producer. """

        await self.producer.stop()
        self._log_message("Producer has closed.")

    def _log_message(self, message):
        """ Helper method to log messages if logging is enabled. """

        if isinstance(self._log, FogVerseLogging):
            self._log.std_log(message)
