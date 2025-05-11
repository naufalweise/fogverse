import asyncio

from fogverse.consumer import KafkaConsumer
from fogverse.producer import KafkaProducer

kafka_server = "localhost:9020"
kafka_topic = "counting"

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
        self.logger.std_log(f"Producing: {message}")
        return message

class MessageConsumer(KafkaConsumer):
    """Consumes and processes messages from a Kafka topic."""

    def __init__(self):
        super().__init__(consumer_server=kafka_server, consumer_topic=kafka_topic)

    def process(self, data):
        """Process the received message."""

        self.logger.std_log(f"Received: {data}")
        return f"Processed: {data}"

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
        await asyncio.gather(
            run_consumer(),
            run_producer()
        )
    except KeyboardInterrupt:
        print("Program interrupted. Stopping now.")

if __name__ == "__main__":
    asyncio.run(main())
