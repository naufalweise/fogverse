import asyncio

from fogverse.consumer.base import BaseConsumer
from fogverse.producer.base import BaseProducer
from fogverse.runnable import Runnable
from fogverse.utils.time import calc_datetime, get_timestamp

class ConsumerStorage(BaseConsumer, BaseProducer, Runnable):
    """A consumer that stores messages in an async queue with optional message retention."""

    def __init__(self, keep_messages=False):
        super().__init__()

        self.queue = asyncio.Queue()
        self.keep_messages = keep_messages
        self._consume_time = None

    def _before_receive(self):
        """Record the timestamp before receiving a message."""

        self._start_time = get_timestamp()

    def _after_receive(self, _):
        """Calculate and store the time taken to consume the message."""

        self._consume_time = calc_datetime(self._start_time)

    def _get_send_extra(self, *args, **kwargs):
        """Attach consumption metadata to the message."""

        return {"consume_time": self._consume_time}

    async def send(self, data):
        """Send data to the queue, replacing the last message if `keep_messages` is False."""

        if not self.keep_messages and not self.queue.empty():
            self.queue.get_nowait() # Discard oldest message if queue isn't empty.

        obj = {
            "data": data,
            "extra": self._get_send_extra(data),
        }
        await self.queue.put(obj)

    async def get(self):
        """Retrieve a message asynchronously."""

        return await self.queue.get()

    def get_nowait(self):
        """Retrieve a message without waiting, or return None if empty."""

        return self.queue.get_nowait() if not self.queue.empty() else None
