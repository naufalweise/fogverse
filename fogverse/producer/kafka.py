import aiokafka
import asyncio
import time

from fogverse.logger import FogLogger
from fogverse.runnable import Runnable

class KafkaProducer(Runnable):
    """Kafka producer using aiokafka with configurable settings."""

    def __init__(self, client_id=f"producer_{int(time.time())}", producer_server="localhost", producer_topic=[], producer_conf={}):
        super().__init__()

        self.client_id = client_id
        self.logger = FogLogger(name=client_id)

        self.producer_topic = producer_topic
        self.producer_conf = {
            "bootstrap_servers": producer_server,
            "client_id": self.client_id,
            **getattr(self, "producer_conf", producer_conf),
        }

        self.producer = aiokafka.AIOKafkaProducer(**self.producer_conf)

    async def start_producer(self):
        """Starts the Kafka producer."""

        await self.producer.start()
        self.logger.std_log(
            f"KAFKA PRODUCER STARTED\nTOPIC: {self.producer_topic}\nCONFIG: {self.producer_conf}"
        )

    async def send(self, data, topic=None, key=None, headers=None, callback=None):
        """Sends a message to the given Kafka topic with optional callback execution."""

        key = key or getattr(self.message, "key", None)
        headers = list(headers or getattr(self.message, "headers", []))
        topic = topic or self.producer_topic

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
        self.logger.std_log("KAFKA PRODUCER CLOSED")
