import asyncio
import signal

from fogverse.consumer import KafkaConsumer
from fogverse.producer import KafkaProducer

kafka_server = "localhost:9092"
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
        self.logger.log_all(f"Producing: {message}")
        return message

class MessageConsumer(KafkaConsumer):
    """Consumes and processes messages from a Kafka topic."""

    def __init__(self):
        super().__init__(consumer_server=kafka_server, consumer_topic=kafka_topic)

    def process(self, data):
        """Process the received message."""

        self.logger.log_all(f"Received: {data}")
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

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def shutdown():
        print("\n[!] Program interrupted by user. Stopping now.")
        stop_event.set()

    loop.add_signal_handler(signal.SIGINT, shutdown)
    loop.add_signal_handler(signal.SIGTERM, shutdown)

    consumer = MessageConsumer()
    producer = MessageProducer()

    consumer_task = asyncio.create_task(consumer.run())
    producer_task = asyncio.create_task(producer.run())

    await stop_event.wait()

    consumer_task.cancel()
    producer_task.cancel()

    try:
        await consumer_task
        await producer_task
    except asyncio.CancelledError:
        pass

if __name__ == "__main__":
    asyncio.run(main())
