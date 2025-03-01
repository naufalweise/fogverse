from .base import AbstractProducer
from aiokafka import AIOKafkaConsumer
from fogverse.runnable import Runnable

class Consumer(AIOKafkaConsumer, AbstractProducer, Runnable):
    """Kafka-based message consumer."""
    pass
