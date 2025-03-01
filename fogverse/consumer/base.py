import cv2

from consumer.storage import ConsumerStorage
from utils.data import bytes_to_numpy
from pickle import UnpicklingError

class AbstractConsumer:
    """Base consumer class with message decoding logic."""

    async def start_consumer(self):
        pass

    async def receive(self):
        raise NotImplementedError

    async def close_consumer(self):
        pass

    def receive_error(self, *args, **kwargs):
        pass

    def decode(self, data):
        """Decodes incoming data based on consumer settings."""
        if not getattr(self, "auto_decode"):
            return data

        # Handle ConsumerStorage format
        if isinstance(getattr(self, "consumer", None), ConsumerStorage):
            self.message, data = data["message"], data["data"]
            self._message_extra = data.get("extra", {})
        try:
            np_arr = bytes_to_numpy(data)
            return cv2.imdecode(np_arr, cv2.IMREAD_COLOR) if np_arr.ndim == 1 else np_arr
        except (OSError, ValueError, TypeError, UnpicklingError):
            pass
        finally:
            return data
