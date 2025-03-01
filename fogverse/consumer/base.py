import cv2

from typing import Any
from consumer.storage import ConsumerStorage
from pickle import UnpicklingError
from utils.data import bytes_to_numpy

class BaseConsumer:
    """Base consumer class with message decoding logic."""

    def __init__(self):
        self.auto_decode: bool = False

    async def start_consumer(self) -> None:
        pass

    async def close_consumer(self) -> None:
        pass

    async def receive(self) -> Any:
        raise NotImplementedError

    def on_receive_error(self) -> None:
        pass

    def decode(self, data):
        """Decodes incoming data based on consumer settings."""
        if not self.auto_decode:
            return data

        # Handle ConsumerStorage message format.
        if isinstance(self.consumer, ConsumerStorage):
            self.message = data["message"]
            payload = data["data"]
            self._message_extra = payload.get("extra", {})
            data = payload["data"]

        # Try decoding as image array.
        try:
            np_arr = bytes_to_numpy(data)
            return cv2.imdecode(np_arr, cv2.IMREAD_COLOR) if np_arr.ndim == 1 else np_arr
        except (OSError, ValueError, TypeError, UnpicklingError):
            pass

        # Fallback to bytes decoding.
        if isinstance(data, bytes):
            try:
                return data.decode()
            except UnicodeDecodeError:
                pass

        return data
