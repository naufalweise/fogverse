from aiokafka import AIOKafkaConsumer
from .base import BaseConsumer

class KafkaConsumer(BaseConsumer):
    """High-performance Kafka consumer with async/await interface."""
    
    def __init__(self, topics: list[str], bootstrap_servers: str, **kwargs):
        super().__init__()
        self.consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            **kwargs
        )
        
    async def start(self):
        await super().start()
        await self.consumer.start()
        
    async def stop(self):
        await super().stop()
        await self.consumer.stop()
        
    async def _receive(self):
        return await self.consumer.getone()
