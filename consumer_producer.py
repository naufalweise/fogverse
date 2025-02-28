import asyncio
import socket
import sys
import uuid
import cv2
from aiokafka import AIOKafkaConsumer as _AIOKafkaConsumer, AIOKafkaProducer as _AIOKafkaProducer
from utils.data import get_config
from .logging import FogVerseLogging
from .base import AbstractConsumer, AbstractProducer


class AIOKafkaConsumer(AbstractConsumer):
    """ Kafka consumer using aiokafka with support for topic patterns and configurable settings. """

    def __init__(self, loop=None):
        self._loop = loop or asyncio.get_event_loop()

        # Get consumer topic(s) from config, ensuring it's a list.
        self._topic_pattern = get_config("TOPIC_PATTERN", self)
        self._consumer_topic = self._parse_topics(get_config("CONSUMER_TOPIC", self, []))

        # Kafka consumer configuration.
        self.consumer_conf = {
            "loop": self._loop,
            "bootstrap_servers": get_config("CONSUMER_SERVERS", self, "localhost"),
            "group_id": get_config("GROUP_ID", self, str(uuid.uuid4())),
            "client_id": get_config("CLIENT_ID", self, socket.gethostname()),
            **getattr(self, "consumer_conf", {}),
        }

        # Initialize the Kafka consumer.
        self.consumer = _AIOKafkaConsumer(*self._consumer_topic, **self.consumer_conf) if self._consumer_topic else _AIOKafkaConsumer(**self.consumer_conf)
        self.seeking_end = None  # Used for handling 'always_read_last' mode.

    @staticmethod
    def _parse_topics(topic):
        """ Ensure topics are stored as a list, even if a single topic is provided as a string. """

        return topic.split(",") if isinstance(topic, str) else topic

    async def start_consumer(self):
        """ Starts the Kafka consumer and subscribes to topics. """

        self._log_message(f"Starting consumer - Topic: {self._consumer_topic}, Config: {self.consumer_conf}")
        await self.consumer.start()

        # Subscribe to topic pattern if provided.
        if self._topic_pattern:
            self.consumer.subscribe(pattern=self._topic_pattern)

        # Give some time to assign partitions.
        await asyncio.sleep(4)

        # If configured, seek to the end of the topic.
        if getattr(self, "read_last", True):
            await self.consumer.seek_to_end()

    async def receive(self):
        """ Fetches a single message from Kafka, handling optional 'always_read_last' logic. """

        # If 'always_read_last' is enabled, seek to the latest offset before reading.
        if getattr(self, "always_read_last", False):
            # If there is a pending seek operation, wait for it to complete.
            if asyncio.isfuture(self.seeking_end):
                await self.seeking_end

            # Start a new seek-to-end operation asynchronously.
            self.seeking_end = asyncio.ensure_future(self.consumer.seek_to_end())

            # If 'always_await_seek_end' is enabled, wait for the seek to finish before continuing.
            if getattr(self, "always_await_seek_end", False):
                await self.seeking_end
                self.seeking_end = None  # Reset seeking state after completion.

        # Fetch a single message from the Kafka topic.
        return await self.consumer.getone()

    async def close_consumer(self):
        """ Gracefully stops the Kafka consumer. """

        await self.consumer.stop()
        self._log_message("Consumer has closed.")

    def _log_message(self, message):
        """ Helper method to log messages if logging is enabled. """

        if isinstance(self._log, FogVerseLogging):
            self._log.std_log(message)

class AIOKafkaProducer(AbstractProducer):
    """ Kafka producer using aiokafka with configurable settings. """

    def __init__(self, loop=None):
        self._loop = loop or asyncio.get_event_loop()

        # Get producer configuration from environment.
        self.producer_topic = get_config("PRODUCER_TOPIC", self)
        self.producer_conf = {
            "loop": self._loop,
            "bootstrap_servers": get_config("PRODUCER_SERVERS", self),
            "client_id": get_config("CLIENT_ID", self, socket.gethostname()),
            **getattr(self, "producer_conf", {}),
        }

        # Initialize the Kafka producer.
        self.producer = _AIOKafkaProducer(**self.producer_conf)

    async def start_producer(self):
        """ Starts the Kafka producer. """

        self._log_message(f"Starting producer - Config: {self.producer_conf}, Topic: {self.producer_topic}")
        await self.producer.start()

    async def _do_send(self, data, topic=None, key=None, headers=None):
        """

        Sends a message to the given Kafka topic.
        If no topic is specified, uses the default producer topic.
        """

        if not (topic := topic or self.producer_topic):
            raise ValueError("Topic should not be None.")

        return await self.producer.send(topic, value=data, key=key, headers=headers)

    async def close_producer(self):
        """ Gracefully stops the Kafka producer. """

        await self.producer.stop()
        self._log_message("Producer has closed.")

    def _log_message(self, message):
        """ Helper method to log messages if logging is enabled. """

        if isinstance(self._log, FogVerseLogging):
            self._log.std_log(message)


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
