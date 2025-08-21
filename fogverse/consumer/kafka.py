import aiokafka
import asyncio
import socket
import time

from fogverse.logger import FogLogger
from fogverse.runnable import Runnable

class KafkaConsumer(Runnable):
    """Kafka consumer using aiokafka with configurable settings."""

    def __init__(self, group_id=socket.gethostname(), client_id=f"consumer_{int(time.time())}", consumer_server="localhost", consumer_topic=[], consumer_conf={}, read_last=False):
        super().__init__()

        self.group_id = group_id
        self.client_id = client_id

        self.logger = FogLogger(name=client_id)

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
        self.read_last = read_last

    @staticmethod
    def _parse_topics(topic):
        """Ensure topics are stored as a list, even if a single topic is provided as a string."""

        return topic.split(",") if isinstance(topic, str) else topic

    async def start_consumer(self):
        """Starts the Kafka consumer and subscribes to topics."""

        await self.consumer.start()
        self.logger.std_log(
            f"KAFKA CONSUMER STARTED\nTOPIC: {self.consumer_topic}\nCONFIG: {self.consumer_conf}"
        )

        # Wait for partition assignment.
        self.logger.std_log("The consumer is waiting for Kafka to assign partitions. Please wait.")
        while not self.consumer.assignment():
            await asyncio.sleep(2)

        self.logger.std_log("Kafka has successfully assigned partitions to the consumer.")

    async def receive(self):
        """Fetches a single message from Kafka."""

        if self.read_last: await self.consumer.seek_to_end()
        return await self.consumer.getone()

    async def close_consumer(self):
        """Gracefully stops the Kafka consumer."""

        await self.consumer.stop()
        self.logger.log_all("KAFKA CONSUMER CLOSED")
