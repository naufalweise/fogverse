import asyncio
import numpy as np

from fogverse.utils.data import numpy_to_bytes
from fogverse.utils.image import compress_encoding

class BaseProducer:
    """Base producer class with message encoding logic."""

    def __init__(self):
        super().__init__()
        self.auto_encode: bool = False

    async def start_producer(self):
        pass

    async def close_producer(self):
        pass

    async def send(self, data, *args, **kwargs) -> asyncio.Future:
        pass

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
