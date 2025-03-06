from fogverse.utils.data import bytes_to_numpy
from fogverse.utils.data import numpy_to_bytes
from fogverse.utils.image import compress_encoding
from pickle import UnpicklingError
from typing import Any
from typing import Any, Optional

import asyncio
import asyncio
import cv2
import numpy as np
import traceback

class Runnable:
    """
    This class acts as a pipeline for receiving, decoding, processing, encoding, and sending messages
    while providing hooks for optional lifecycle methods.
    """

    def __init__(self):
        super().__init__()
        self.message: Optional[Any] = None

        self.auto_decode: bool = False
        self.auto_encode: bool = False

        self._started = False
        self._closed = False

    async def start_consumer(self): pass
    async def close_consumer(self): pass

    async def start_producer(self): pass
    async def close_producer(self): pass

    async def receive(self) -> Any: pass
    async def send(self, data, *args, **kwargs) -> asyncio.Future: pass

    def decode(self, data):
        """Decodes incoming data appropriately."""

        if not self.auto_decode: return data

        # NOTE: This is legacy code and may require updates in the future.  
        # This handles the ConsumerStorage message format.  
        from fogverse.consumer import ConsumerStorage

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

    def on_error(self, exception: Exception) -> None:
        """Handles exceptions by printing the traceback."""

        traceback.print_exc()

    def process(self, data: Any) -> Any:
        """Processes the incoming data (override in subclasses)."""

        return data

    async def _start(self) -> None:
        """Initializes the consumer and producer if they haven"t started yet."""

        if not self._started:
            await self.start_consumer()
            await self.start_producer()

            self._started = True

    async def _close(self) -> None:
        """Closes the consumer and producer if they haven"t been closed yet."""

        if not self._closed:
            await self.close_producer()
            await self.close_consumer()

            self._closed = True

    async def run(self):
        """Main loop that continuously processes messages."""

        try:
            await self._call_optional("_before_start")
            await self._start()
            await self._call_optional("_after_start")

            while True:
                await self._process_message()

        except Exception as e:
            self.on_error(e)
        finally:
            await self._call_optional("_before_close")
            await self._close()
            await self._call_optional("_after_close")

    async def stop(self):
        await self._call_optional("_before_close")
        await self._close()
        await self._call_optional("_after_close")

    async def _process_message(self):
        """Process a single message through the complete pipeline."""

        await self._call_optional("_before_receive")
        self.message = await self.receive()

        if self.message is None:
            return  # No message to process.

        await self._call_optional("_after_receive", self.message)

        # Extract the message (compatible with Kafka and OpenCV).
        value = self._extract_value(self.message)

        # Decode the message.
        await self._call_optional("_before_decode", value)
        data = self.decode(value)
        await self._call_optional("_after_decode", data)

        # Process the message.
        await self._call_optional("_before_process", data)
        result = await data if asyncio.iscoroutine(data) else self.process(data)
        await self._call_optional("_after_process", result)

        # Encode the processed message.
        await self._call_optional("_before_encode", result)
        bytes = self.encode(result)
        await self._call_optional("_after_encode", bytes)

        # Send the final message.
        await self._call_optional("_before_send", bytes)
        await self.send(bytes)
        await self._call_optional("_after_send", bytes)
        return sent

    async def _call_optional(self, method, *args):
        """
        Calls an optional lifecycle method if it exists.
        If the method is asynchronous, it awaits it. Otherwise, it calls it normally.
        """

        func = getattr(self, method, None)
        if callable(func):
            return await func(*args) if asyncio.iscoroutinefunction(func) else func(*args)

    def _extract_value(self, message):
        """
        Extracts the message value, handling both Kafka and OpenCV formats.

        - Kafka messages have a `.value` method or property.
        - OpenCV messages are already raw data.
        """

        if hasattr(message, "value"):
            return message.value() if callable(message.value) else message.value
        return message  # Any other message.
