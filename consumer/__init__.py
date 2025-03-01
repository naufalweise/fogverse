from .base import AbstractProducer
from aiokafka import AIOKafkaConsumer
from runnable import Runnable

class Consumer(AIOKafkaConsumer, AbstractProducer, Runnable):
    """Kafka-based message consumer."""
    pass
