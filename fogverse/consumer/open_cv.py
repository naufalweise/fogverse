import asyncio
import cv2

from concurrent.futures import ThreadPoolExecutor
from fogverse.consumer.storage import ConsumerStorage
from fogverse.utils.data import get_config

class ConsumerOpenCV(ConsumerStorage):
    """Video frame consumer using OpenCV. Reads from a camera or video device."""

    def __init__(self, executor=ThreadPoolExecutor(max_workers=1)):
        super().__init__()

        self._device = get_config("DEVICE", default=0)
        self._executor = executor
        self.consumer = getattr(self, "consumer", None) or cv2.VideoCapture(self._device)

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
