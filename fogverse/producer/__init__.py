from aiokafka import AIOKafkaProducer
from consumer.base import AbstractConsumer
from runnable import Runnable

class Producer(AbstractConsumer, AIOKafkaProducer, Runnable):
    """Kafka-based message producer."""
    pass
