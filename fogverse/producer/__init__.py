from aiokafka import AIOKafkaProducer
from consumer.base import BaseConsumer
from fogverse.runnable import Runnable

class Producer(BaseConsumer, AIOKafkaProducer, Runnable):
    """Kafka-based message producer."""
    pass
