import time
import aiokafka
import asyncio
import socket

from fogverse.logger import FogLogger
from fogverse.runnable import Runnable

class KafkaConsumer(Runnable):
    """Kafka consumer using aiokafka with configurable settings."""

    def __init__(self, group_id=socket.gethostname(), client_id=f"consumer_{int(time.time())}", consumer_server="localhost", consumer_topic=[], consumer_conf={}, read_last=False):
        super().__init__()

        self.group_id = group_id
        self.client_id = client_id

        self.logger = FogLogger(name=client_id)
        self.read_last = read_last

        self.consumer_topic = self._parse_topics(consumer_topic)
        self.consumer_conf = {
            "bootstrap_servers": consumer_server,
            "group_id": self.group_id,
            "client_id": self.client_id,
            **getattr(self, "consumer_conf", consumer_conf),
        }

        self.consumer = aiokafka.AIOKafkaConsumer(
            *self.consumer_topic,
            **self.consumer_conf
        )

        if read_last:
            self.seeking_end = asyncio.ensure_future(self.consumer.seek_to_end())

    @staticmethod
    def _parse_topics(topic):
        """Ensure topics are stored as a list, even if a single topic is provided as a string."""

        return topic.split(",") if isinstance(topic, str) else topic

    async def start_consumer(self):
        """Starts the Kafka consumer and subscribes to topics."""

        self.logger.std_log(f"KAFKA CONSUMER START - TOPIC: {self.consumer_topic}, CONFIG: {self.consumer_conf}")
        await self.consumer.start()

        # Give some time to assign partitions.
        await asyncio.sleep(4)

    async def receive(self):
        """Fetches a single message from Kafka."""

        if self.read_last and asyncio.isfuture(self.seeking_end):
            await self.seeking_end
        return await self.consumer.getone()

    async def close_consumer(self):
        """Gracefully stops the Kafka consumer."""

        await self.consumer.stop()
        self.logger.std_log("Consumer has closed.")
