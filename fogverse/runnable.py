import asyncio
import asyncio
import cv2
import numpy as np
import traceback

from fogverse.utils.data import bytes_to_numpy
from fogverse.utils.data import numpy_to_bytes
from fogverse.utils.image import compress_encoding
from pickle import UnpicklingError
from typing import Any
from typing import Any, Optional

class Runnable:
    """
    This class acts as a pipeline for receiving, decoding, processing, encoding, and sending messages
    and lets you plug in optional steps to run before or after each stage.
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
        """
        Decodes incoming data.
        """

        if not self.auto_decode:
            # Skip decoding if auto_decode is disabled.
            return data

        # NOTE: This is legacy code and may require updates in the future.  
        # This handles the ConsumerStorage message format.  
        from fogverse.consumer import ConsumerStorage
        if isinstance(self.consumer, ConsumerStorage):
            self.message = data["message"]
            data = data["data"]["data"]

        # Attempt to decode as an image.
        try:
            arr = bytes_to_numpy(data)
            # If 1D, treat it as raw-encoded image bytes (e.g., JPEG/PNG).
            return cv2.imdecode(arr, cv2.IMREAD_COLOR) if arr.ndim == 1 else arr
        except Exception:
            pass  # Not an image or bad array.

        # Fallback to string decoding if it's bytes.
        if isinstance(data, bytes):
            try:
                return data.decode()
            except Exception:
                pass  # Not decodable as UTF-8.

        # If all else fails return the data unchanged.
        return data

    def encode(self, data):
        """
        Encodes outgoing data.
        """

        if not self.auto_encode:
            # Skip encoding if auto_encode is disabled.
            return data
        else:
            match data:
                case bytes():
                    # Already bytes, no need to encode.
                    return data
                case str():
                    # If it's a string, encode it to bytes.
                    # This is useful for Kafka producers.
                    return data.encode()
                case list() | tuple():
                    # If it's a list or tuple, convert it to a NumPy array.
                    # This is useful for OpenCV producers.
                    data = np.array(data)
                case _ if type(data).__name__ == "Tensor":
                    # If it's a PyTorch tensor, convert it to a NumPy array.
                    # This is useful for PyTorch producers.
                    data = data.cpu().numpy()
                case np.ndarray():
                    # If it's already a NumPy array, compress or convert it to bytes.
                    return compress_encoding(data, getattr(self, "encoding", None)) or numpy_to_bytes(data)
            # If it's not a recognized type, convert it to bytes.
            return bytes(data)

    def on_error(self, exception: Exception) -> None:
        """
        Handles exceptions by printing the traceback.
        """

        print("\n[!] Uh-oh, something went wrong:")
        print(f"{exception}\n")
        traceback.print_exc()

    def process(self, data: Any) -> Any:
        """
        Define your data processing logic here. Defaults to returning the data as-is.
        """

        return data

    async def _start(self) -> None:
        """
        Initializes the consumer and producer if they haven't started yet.
        """

        if not self._started:
            await self.start_consumer()
            await self.start_producer()

            self._started = True

    async def _close(self) -> None:
        """
        Closes the consumer and producer if they haven't been closed yet.
        """

        if not self._closed:
            await self.close_producer()
            await self.close_consumer()

            self._closed = True

    async def run(self):
        """
        This method is the main entry point for running the pipeline.
        It initializes the consumer and producer, executes messages handling pipeline in a loop.
        """

        try:
            await self._call_optional("_before_start")
            await self._start()
            await self._call_optional("_after_start")

            while True:
                await self._handle_message()

        except Exception as e:
            self.on_error(e)
        finally:
            await self._call_optional("_before_close")
            await self._close()
            await self._call_optional("_after_close")

    async def stop(self):
        """
        Stops the processing loop and closes the consumer and producer.
        """

        await self._call_optional("_before_close")
        await self._close()
        await self._call_optional("_after_close")

    async def _handle_message(self):
        """
        Handles a single message through the complete pipeline.
        """

        await self._call_optional("_before_receive")
        self.message = await self.receive()
        await self._call_optional("_after_receive", self.message)

        if self.message is None:
            return  # No message to process.

        extracted = self._extract(self.message)

        await self._call_optional("_before_decode", extracted)
        decoded = self.decode(extracted)
        await self._call_optional("_after_decode", decoded)

        await self._call_optional("_before_process", decoded)
        processed = await decoded if asyncio.iscoroutine(decoded) else self.process(decoded)
        await self._call_optional("_after_process", processed)

        await self._call_optional("_before_encode", processed)
        encoded = self.encode(processed)
        await self._call_optional("_after_encode", encoded)

        await self._call_optional("_before_send", encoded)
        await self.send(encoded)
        await self._call_optional("_after_send", encoded)

    async def _call_optional(self, method, *args):
        """
        Calls an optional method in this class or its subclasses with the given arguments.
        If the method is asynchronous, it awaits it. Otherwise, it calls it normally.
        """

        # Check if the method exists and is callable.
        func = getattr(self, method, None)

        if callable(func):
            # Call the optional method.
            return await func(*args) if asyncio.iscoroutinefunction(func) else func(*args)

    def _extract(self, message):
        """
        Extracts the value from the message. This is useful for handling different message formats.
        For example, in Kafka, the message may have a 'value' attribute that contains the actual data.
        In OpenCV, the message may be a NumPy array or a list of arrays.
        """

        if hasattr(message, "value"):
            # If the message has a 'value' attribute, extract it.
            return message.value() if callable(message.value) else message.value
        return message  # Else return the message as is if it doesn't have a 'value' attribute.
