import asyncio
import cv2

from ..logger import FogVerseLogging
from consumer.base import AbstractConsumer
from utils.data import get_config

class OpenCVConsumer(AbstractConsumer):
    """ Video frame consumer using OpenCV. Reads from a camera or video device. """

    def __init__(self, loop=None, executor=None):
        self._device = get_config("DEVICE", self, 0)

        # Initialize OpenCV video capture device.
        self.consumer = getattr(self, "consumer", None) or cv2.VideoCapture(self._device)
        self._loop = loop or asyncio.get_event_loop()
        self._executor = executor

    def close_consumer(self):
        """ Releases the OpenCV video capture device. """

        self.consumer.release()
        self._log_message("OpenCVConsumer has closed.")

    async def receive(self):
        """ Reads a frame from the video capture. If reading fails, handle the error. """

        success, frame = self.consumer.read()
        return frame if success else await self._handle_receive_error()

    async def _handle_receive_error(self):
        """ Handles video capture errors and exits the process. """

        self._log_message("OpenCVConsumer has stopped due to an error.")
        sys.exit(0)

    def _log_message(self, message):
        """ Helper method to log messages if logging is enabled. """

        if isinstance(self._log, FogVerseLogging):
            self._log
