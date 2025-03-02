import asyncio
from concurrent.futures import ThreadPoolExecutor
import cv2
import os

from fogverse.consumer import ConsumerStorage
from fogverse.producer import KafkaProducer
from fogverse.utils.data import numpy_to_bytes

# Set environment variables.
os.environ["PRODUCER_SERVERS"] = "localhost:9092"
os.environ["PRODUCER_TOPIC"] = "processed-frames"

class OpenCV(ConsumerStorage):
    """Captures frames from a video source."""

    def __init__(self, device_id=0):
        super().__init__(keep_messages=False)
        self.cap = cv2.VideoCapture(device_id)
        self.executor = ThreadPoolExecutor(max_workers=1)

    async def receive(self):
        """Capture a frame from the camera."""

        loop = asyncio.get_event_loop()
        success, frame = await loop.run_in_executor(
            self.executor,
            self.cap.read
        )
        return frame if success else None

    async def close_consumer(self):
        """Release the camera when done."""

        if hasattr(self, "cap") and self.cap.isOpened():
            self.cap.release()
        self.executor.shutdown()

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
        camera_consumer = OpenCV(device_id=0)
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
