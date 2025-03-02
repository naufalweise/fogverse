import aiokafka
import asyncio
import socket
import uuid

from .base import BaseConsumer
from fogverse.logger import FogLogger
from fogverse.producer.base import BaseProducer
from fogverse.runnable import Runnable
from fogverse.utils.data import get_config

class KafkaConsumer(BaseConsumer, BaseProducer, Runnable):
    """Kafka consumer using aiokafka with support for topic patterns and configurable settings."""

    def __init__(self, read_last=False):
        super().__init__()

        # Create consumer ID
        group_id = get_config("GROUP_ID", default=socket.gethostname())
        client_id = get_config("CLIENT_ID", default=str(uuid.uuid4()))

        # Create a logger instance.
        self._log = FogLogger(name=client_id)
        self.read_last = read_last

        # Explicit list of topics to consume from.
        self._consumer_topic = self._parse_topics(get_config("CONSUMER_TOPIC", default=[]))

        # Kafka consumer configuration.
        self.consumer_conf = {
            "bootstrap_servers": get_config("CONSUMER_SERVERS", default="localhost"),
            "group_id": group_id,
            "client_id": client_id,
            **getattr(self, "consumer_conf", {}),
        }

        # Initialize the Kafka consumer.
        self.consumer = aiokafka.AIOKafkaConsumer(
            *self._consumer_topic,
            **self.consumer_conf
        )

        if read_last: self.seeking_end = asyncio.ensure_future(self.consumer.seek_to_end())

    @staticmethod
    def _parse_topics(topic):
        """Ensure topics are stored as a list, even if a single topic is provided as a string."""

        return topic.split(",") if isinstance(topic, str) else topic

    async def start_consumer(self):
        """Starts the Kafka consumer and subscribes to topics."""

        self._log.std_log(f"KAFKA CONSUMER START - TOPIC: {self._consumer_topic}, CONFIG: {self.consumer_conf}")
        await self.consumer.start()

        # Give some time to assign partitions.
        await asyncio.sleep(4)

    async def receive(self):
        """Fetches a single message from Kafka."""

        if self.read_last and asyncio.isfuture(self.seeking_end): await self.seeking_end
        return await self.consumer.getone()

    async def close_consumer(self):
        """Gracefully stops the Kafka consumer."""

        await self.consumer.stop()
        self._log.std_log("Consumer has closed.")
