import asyncio
import numpy as np

from ..utils.data import numpy_to_bytes
from ..utils.image import compress_encoding

class BaseProducer:
    """Base producer class with message encoding logic."""

    def __init__(self):
        super().__init__()
        self.auto_encode: bool = False

    async def start_producer(self):
        pass

    async def close_producer(self):
        pass

    async def _do_send(self, data, *args, **kwargs) -> asyncio.Future:
        raise NotImplementedError

    def encode(self, data):
        """Encodes outgoing data based on producer settings."""

        if not self.auto_encode:
            return data
        else:
            match data:
                case bytes():
                    return data
                case str():
                    return data.encode()
                case list() | tuple():
                    data = np.array(data)
                case _ if type(data).__name__ == "Tensor":
                    data = data.cpu().numpy()
                case np.ndarray():
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
            result = future if future else None
            res = callback(result, *self._get_extra_callback_args() if hasattr(self, "_get_extra_callback_args") else ())
            return await res if asyncio.iscoroutine(res) else res

        return asyncio.ensure_future(_call_callback_ack())  # Return an awaitable future.
