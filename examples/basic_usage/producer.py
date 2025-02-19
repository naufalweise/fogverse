from fogverse.producer.kafka import KafkaProducer
from fogverse.utils.logging import FogLogger
from fogverse.admin import create_topics

import asyncio

class MyProducer(KafkaProducer):
    def __init__(self):
        super().__init__(
            topic="my-topic",
            bootstrap_servers="localhost:9092"
        )
        self.logger = FogLogger("MyProducer")
        
    async def produce(self, data: str):
        await self.send(data.encode())
        self.logger.info(f"Sent: {data}")

async def main():
    # Create topic.
    kafka_config = {
        "bootstrap_servers": "localhost:9092",
        "topics": [
            {"name": "my-topic", "partitions": 1}
        ]
    }
    await create_topics(kafka_config)
    
    producer = MyProducer()
    await producer.start()
    for i in range(10):
        await producer.produce(f"Message {i}")
    await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())
