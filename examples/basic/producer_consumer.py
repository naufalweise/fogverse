import asyncio

from fogverse.consumer import KafkaConsumer
from fogverse.producer import KafkaProducer

# Set variables for Kafka connections.
kafka_server = "localhost:9092"
kafka_topic = "pre-processed"

class MessageProducer(KafkaProducer):
    """Produces messages to a Kafka topic."""
    
    def __init__(self):
        super().__init__(producer_server=kafka_server, producer_topic=kafka_topic)
        self.counter = 0

    async def receive(self):
        """Generate a new message every second."""

        await asyncio.sleep(1)
        message = f"Message {self.counter}"
        self.counter += 1
        print(f"Producing: {message}")
        return message
    
    def encode(self, data):
        """Convert string to bytes."""

        return data.encode()

class MessageConsumer(KafkaConsumer):
    """Consumes and processes messages from a Kafka topic."""

    def __init__(self):
        super().__init__(consumer_server=kafka_server, consumer_topic=kafka_topic)
        self.producer_topic = "post-processed"

    def decode(self, data):
        """Convert bytes to string."""

        if isinstance(data, bytes):
            return data.decode()
        return data

    def process(self, data):
        """Process the received message."""

        print(f"Received: {data}")
        return f"Processed: {data}"

    def encode(self, data):
        """Convert processed string to bytes."""

        return data.encode()

    async def send(self, _):
        pass
    
async def run_producer():
    """Run the producer."""
    producer = MessageProducer()
    await producer.run()

async def run_consumer():
    """Run the consumer."""
    consumer = MessageConsumer()
    await consumer.run()

async def main():
    """Run producer and consumer concurrently."""

    try:
        # Run both components at the same time.
        await asyncio.gather(
            run_producer(),
            run_consumer()
        )
    except KeyboardInterrupt:
        print("Stopping...")

if __name__ == "__main__":
    asyncio.run(main())
