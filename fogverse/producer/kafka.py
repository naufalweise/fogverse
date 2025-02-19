from aiokafka import AIOKafkaProducer
from .base import BaseProducer

class KafkaProducer(BaseProducer):
    """High-performance Kafka producer with async/await interface."""
    
    def __init__(self, topic: str, bootstrap_servers: str, **kwargs):
        super().__init__()
        self.topic = topic
        self.producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            **kwargs
        )
        
    async def start(self):
        await super().start()
        await self.producer.start()
        
    async def stop(self):
        await super().stop()
        await self.producer.stop()
        
    async def _send(self, data: bytes):
        await self.producer.send(self.topic, value=data)
