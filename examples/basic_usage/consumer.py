import asyncio
from fogverse.consumer.kafka import KafkaConsumer
from fogverse.utils.logging import FogLogger

class MyConsumer(KafkaConsumer):
    def __init__(self):
        super().__init__(
            topics=["my-topic"],
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest"  # Start from the beginning of the topic.
        )
        self.logger = FogLogger("MyConsumer")
        
    async def process(self, message: str):
        """Process the decoded message."""
        self.logger.info(f"Received: {message}")

async def main():
    consumer = MyConsumer()
    await consumer.start()
    try:
        while True:
            message = await consumer.receive()
            await consumer.process(message)
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(main())
