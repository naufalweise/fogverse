import asyncio
import signal
import time

from fogverse.producer import KafkaProducer
from monitor import start_iostat_stream, get_resource_usage_snapshot

kafka_server = "localhost:9092"
kafka_topic = "prod-throughput"

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

async def run_producer():
    """Run the producer."""
    producer = MessageProducer()
    await producer.run()

async def main():
    """Run producer and consumer concurrently."""

    if not start_iostat_stream():
        raise RuntimeError("Failed to start iostat")

    try:
        while True:
            usage = get_resource_usage_snapshot("test-container-0")
            print(usage)
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopped.")

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def shutdown():
        print("\n[!] Program interrupted by user. Stopping now.")
        stop_event.set()

    loop.add_signal_handler(signal.SIGINT, shutdown)
    loop.add_signal_handler(signal.SIGTERM, shutdown)

    producer = MessageProducer()

    producer_task = asyncio.create_task(producer.run())

    await stop_event.wait()

    producer_task.cancel()

    try:
        await producer_task
    except asyncio.CancelledError:
        pass

if __name__ == "__main__":
    asyncio.run(main())
