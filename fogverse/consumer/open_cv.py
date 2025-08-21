import asyncio
import cv2

from concurrent.futures import ThreadPoolExecutor
from fogverse.consumer.storage import ConsumerStorage

class ConsumerOpenCV(ConsumerStorage):
    """Video frame consumer using OpenCV. Reads from a camera or video device."""

    def __init__(self, device: int=0, executor=ThreadPoolExecutor(max_workers=1)):
        super().__init__()

        self.device = device
        self.consumer = getattr(self, "consumer", None) or cv2.VideoCapture(self.device)

        self._executor = executor

    def close_consumer(self):
        """Releases the OpenCV video capture device."""

        self.consumer.release()
        self._executor.shutdown()

    async def receive(self):
        """Capture a frame."""

        loop = asyncio.get_event_loop()
        success, frame = await loop.run_in_executor(
            self._executor,
            self.consumer.read
        )
        return frame if success else None
