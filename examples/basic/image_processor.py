import asyncio
import cv2

from fogverse.consumer import ConsumerStorage
from fogverse.consumer.open_cv import ConsumerOpenCV
from fogverse.producer import KafkaProducer

kafka_server = "localhost:9092"
kafka_topic = "cam-frames"

class FrameProcessor(KafkaProducer):
    """Processes video frames and sends them to Kafka."""

    def __init__(self, storage: ConsumerStorage):
        super().__init__(producer_server=kafka_server, producer_topic=kafka_topic)
        self.storage = storage

    async def receive(self):
        obj = await self.storage.get()
        return obj["data"] if obj else None

    def process(self, frame):
        """Process the frame (example: apply grayscale)."""

        if frame is None: return None

        # Convert to grayscale then back to BGR for visualization.
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        processed = cv2.cvtColor(gray, cv2.COLOR_GRAY2BGR)

        # Display the processed frame.
        cv2.imshow("Processed Frame", processed)
        cv2.waitKey(1)

        return processed

async def main():
    """Set up and run the image processing pipeline."""

    try:
        # Create components.
        camera_consumer = ConsumerOpenCV()
        processor = FrameProcessor(camera_consumer)

        # Start both components.
        await asyncio.gather(
            camera_consumer.run(),
            processor.run()
        )

    except KeyboardInterrupt:
        print("Program interrupted. Stopping now.")
        cv2.destroyAllWindows()

if __name__ == "__main__":
    asyncio.run(main())
