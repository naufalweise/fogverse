import asyncio
import numpy as np

from utils.data import numpy_to_bytes
from utils.image import compress_encoding

class BaseProducer:
    """Base producer class with message encoding logic."""

    async def start_producer(self):
        pass

    async def close_producer(self):
        pass

    async def _do_send(self, data, *args, **kwargs) -> asyncio.Future:
        raise NotImplementedError

    def encode(self, data):
        """Encodes outgoing data based on producer settings."""

        if isinstance(data, bytes):
            return data
        if not getattr(self, "auto_encode", True):
            return data
        if isinstance(data, str):
            return data.encode()
        if isinstance(data, (list, tuple)):
            data = np.array(data)
        if type(data).__name__ == "Tensor":
            data = data.cpu().numpy()
        if isinstance(data, np.ndarray):
            return compress_encoding(data, getattr(self, "encode_encoding", None)) or numpy_to_bytes(data)
        return bytes(data)

    async def send(self, data, topic=None, key=None, headers=None, callback=None):
        """Sends data with optional callback execution."""

        key = key or getattr(self.message, "key", None)
        headers = list(headers or getattr(self.message, "headers", []))
        topic = topic or getattr(self, "producer_topic", "")

        future = await self._do_send(data, topic=topic, key=key, headers=headers)

        if not callable(callback := callback or getattr(self, "callback", None)):
            return future

        async def _call_callback_ack():
            result = await future if future else None
            res = callback(result, *self._get_extra_callback_args() if hasattr(self, "_get_extra_callback_args") else ())
            return await res if asyncio.iscoroutine(res) else res

        return asyncio.ensure_future(_call_callback_ack())  # Return an awaitable future.
