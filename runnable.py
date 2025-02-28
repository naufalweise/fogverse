import asyncio
import traceback

class Runnable:
    """
    Base class for an asynchronous processing pipeline.

    This class handles consuming, processing, encoding, and sending messages
    while providing hooks for optional lifecycle methods.
    """

    def on_error(self, _):
        """Handles exceptions by printing the traceback."""
        traceback.print_exc()

    def process(self, data):
        """Processes the incoming data (override in subclasses)."""
        return data

    async def _start(self):
        """Initializes the consumer and producer if they haven't started yet."""
        if getattr(self, '_started', False):
            return
        await self._call_optional('start_consumer')
        await self._call_optional('start_producer')
        self._started = True

    async def _close(self):
        """Closes the consumer and producer if they haven't been closed yet."""
        if getattr(self, '_closed', False):
            return
        await self._call_optional('close_producer')
        await self._call_optional('close_consumer')
        self._closed = True

    async def run(self):
        """Main loop that continuously receives, processes, and sends messages."""
        try:
            await self._call_optional('_before_start')
            await self._start()
            await self._call_optional('_after_start')

            while True:
                await self._process_message()
                
        except Exception as e:
            self.on_error(e)
        finally:
            await self._call_optional('_before_close')
            await self._close()
            await self._call_optional('_after_close')

    async def _process_message(self):
        """Handles the complete lifecycle of a single message: receive → process → send."""
        await self._call_optional('_before_receive')
        self.message = await self.receive()
        
        if self.message is None:  # Skip if no message is received.
            return
        
        await self._call_optional('_after_receive', self.message)

        # Extract the message payload (compatible with Kafka and OpenCV).
        value = self._extract_value(self.message)

        # Decode the message.
        await self._call_optional('_before_decode', value)
        data = self.decode(value)
        await self._call_optional('_after_decode', data)

        # Process the message.
        await self._call_optional('_before_process', data)
        result = await data if asyncio.iscoroutine(data) else self.process(data)
        await self._call_optional('_after_process', result)

        # Encode the processed message.
        await self._call_optional('_before_encode', result)
        result_bytes = self.encode(result)
        await self._call_optional('_after_encode', result_bytes)

        # Send the final message.
        await self._call_optional('_before_send', result_bytes)
        await self.send(result_bytes)
        await self._call_optional('_after_send', result_bytes)

    async def _call_optional(self, method, *args):
        """Calls an optional lifecycle method if it exists.

        If the method is asynchronous, it awaits it. Otherwise, it calls it normally.
        """
        func = getattr(self, method, None)
        if callable(func):
            return await func(*args) if asyncio.iscoroutinefunction(func) else func(*args)

    def _extract_value(self, message):
        """Extracts the message value, handling both Kafka and OpenCV formats.

        - Kafka messages have a `.value` method or property.
        - OpenCV messages are already raw data.
        """
        if hasattr(message, "value"):
            return message.value() if callable(message.value) else message.value
        return message
