import asyncio
import cv2
import os

from fogverse.consumer import ConsumerStorage
from fogverse.consumer.open_cv import ConsumerOpenCV
from fogverse.producer import KafkaProducer
from fogverse.utils.data import numpy_to_bytes

# Set environment variables.
os.environ["PRODUCER_SERVERS"] = "localhost:9092"
os.environ["PRODUCER_TOPIC"] = "processed-frames"

class FrameProcessor(KafkaProducer):
    """Processes video frames and sends them to Kafka."""

    def __init__(self, storage: ConsumerStorage):
        super().__init__()
        self.storage = storage

    async def receive(self):
        obj = await self.storage.get()
        return obj["data"] if obj else None

    def process(self, frame):
        """Process the frame (example: apply grayscale)."""

        if frame is None:
            return None

        # Convert to grayscale then back to BGR for visualization.
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        processed = cv2.cvtColor(gray, cv2.COLOR_GRAY2BGR)

        # Display the processed frame.
        cv2.imshow('Processed Frame', processed)
        cv2.waitKey(1)

        return processed

    def encode(self, frame):
        """Convert NumPy array to bytes."""

        if frame is None:
            return b''
        return numpy_to_bytes(frame)

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
        print("Stopping...")
        cv2.destroyAllWindows()

if __name__ == "__main__":
    asyncio.run(main())
