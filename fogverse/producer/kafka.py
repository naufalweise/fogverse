import uuid
import aiokafka
import asyncio

from fogverse.consumer.base import BaseConsumer
from fogverse.logger import FogLogger
from fogverse.producer.base import BaseProducer
from fogverse.runnable import Runnable
from fogverse.utils.data import get_config

class KafkaProducer(BaseProducer, BaseConsumer, Runnable):
    """Kafka producer using aiokafka with configurable settings."""

    def __init__(self, client_id=str(uuid.uuid4()), producer_topic=[], producer_server="localhost", producer_conf={}):
        super().__init__()

        # Create consumer ID
        self.client_id = get_config("CLIENT_ID", default=client_id)

        # Create a logger instance.
        self._log = FogLogger(name=client_id)

        # Explicit list of topics to produce to.
        self.producer_topic = get_config("PRODUCER_TOPIC", default=producer_topic)

        # Kafka producer configuration.
        self.producer_conf = {
            "bootstrap_servers": get_config("PRODUCER_SERVERS", default=producer_server),
            "client_id": self.client_id,
            **getattr(self, "producer_conf", producer_conf),
        }

        # Initialize the Kafka producer.
        self.producer = aiokafka.AIOKafkaProducer(**self.producer_conf)

    async def start_producer(self):
        """Starts the Kafka producer."""

        self._log.std_log(f"KAFKA PRODUCER START - TOPIC: {self.producer_topic}, CONFIG: {self.producer_conf}")
        await self.producer.start()

    async def send(self, data, topic=None, key=None, headers=None, callback=None):
        """
        Sends a message to the given Kafka topic with optional callback execution.
        """

        key = key or getattr(self.message, "key", None)
        headers = list(headers or getattr(self.message, "headers", []))
        topic = topic or getattr(self, "producer_topic", "")

        if not (topic := topic or self.producer_topic):
            raise ValueError("Topic should not be None.")
        future = await self.producer.send(topic, value=data, key=key, headers=headers)

        if not callable(callback := callback or getattr(self, "callback", None)):
            return future

        async def _call_callback_ack():
            result = future if future else None
            res = callback(result, *self._get_extra_callback_args() if hasattr(self, "_get_extra_callback_args") else ())
            return await res if asyncio.iscoroutine(res) else res

        return asyncio.ensure_future(_call_callback_ack())  # Return an awaitable future.

    async def close_producer(self):
        """Gracefully stops the Kafka producer."""

        await self.producer.stop()
        self._log.std_log("Producer has closed.")
