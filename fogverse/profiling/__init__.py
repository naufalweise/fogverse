import asyncio
import json
import logging
import os
import secrets
import socket

from aiokafka import ConsumerRecord, AIOKafkaProducer
from fogverse.logger import FogLogger
from ..utils.time import calc_datetime, get_timestamp, format_timestamp
from ..utils.data import get_header, get_mem_size

class AbstractProfiling:
    """Defines hook methods for profiling lifecycle events."""
    def _before_start(self, *args, **kwargs): ...
    def _after_start(self, *args, **kwargs): ...
    def _before_receive(self, *args, **kwargs): ...
    def _after_receive(self, *args, **kwargs): ...
    def _before_decode(self, *args, **kwargs): ...
    def _after_decode(self, *args, **kwargs): ...
    def _before_process(self, *args, **kwargs): ...
    def _after_process(self, *args, **kwargs): ...
    def _before_encode(self, *args, **kwargs): ...
    def _after_encode(self, *args, **kwargs): ...
    def _before_send(self, *args, **kwargs): ...
    def _after_send(self, *args, **kwargs): ...
    def _before_close(self, *args, **kwargs): ...
    def _after_close(self, *args, **kwargs): ...

class BaseProfiling(AbstractProfiling):
    """Base class for profiling execution times and data sizes."""
    
    def __init__(self, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._log_data = {}

    def finalize_data(self, data=None, default_value=None):
        """Prepare final log data."""
        data = data or self._log_data
        return self._df_header, [data.get(header, default_value) for header in self._df_header]


class Profiling(BaseProfiling):
    """Profiles execution times, data sizes, and logs statistics locally or remotely."""

    DEFAULT_HEADERS = [
        "topic from", "topic to", "frame", "offset received",
        "frame delay (ms)", "msg creation delay (ms)",
        "consume time (ms)", "size data received (KB)",
        "decode time (ms)", "size data decoded (KB)",
        "process time (ms)", "size data processed (KB)",
        "encode time (ms)", "size data encoded (KB)",
        "send time (ms)", "size data sent (KB)", "offset sent"
    ]

    def __init__(self, name=None, dirname=None, remote_logging=False,
                 remote_logging_name=None, logger=None, app_id=None, loop=None):
        super().__init__(loop)
        self._unique_id = secrets.token_hex(3)
        self.app_id = app_id or self._unique_id
        self._profiling_name = name or f"{self.__class__.__name__}_{self._unique_id}"
        self._df_header = self.DEFAULT_HEADERS
        self._log = logger or FogLogger(self._profiling_name, dirname, self._df_header, logging.FOGV_CSV)

        # Set up remote logging if enabled.
        self._logging_producer = None
        if remote_logging:
            self._setup_remote_logging(remote_logging_name)

    def _setup_remote_logging(self, remote_logging_name):
        """Initialize remote logging configurations."""
        self._remote_logging_name = remote_logging_name or self._profiling_name
        self._logging_topic = os.getenv("LOGGING_TOPIC", "fogverse-profiling")
        self._logging_producer_servers = os.getenv("LOGGING_PRODUCER_SERVERS") or os.getenv("PRODUCER_SERVERS")

        if not self._logging_producer_servers:
            raise ValueError("Remote logging requires a Kafka server. Set LOGGING_PRODUCER_SERVERS or PRODUCER_SERVERS.")

        self._logging_producer = AIOKafkaProducer(
            loop=self._loop,
            bootstrap_servers=self._logging_producer_servers,
            client_id=os.getenv("CLIENT_ID", socket.gethostname())
        )

    async def start_producer(self):
        """Start Kafka producer for logging."""
        if hasattr(super(), "start_producer"):
            await super().start_producer()
        if self._logging_producer:
            await self._logging_producer.start()

    async def stop_producer(self):
        """Stop Kafka producer for logging."""
        if hasattr(super(), "stop_producer"):
            await super().stop_producer()
        if self._logging_producer:
            await self._logging_producer.stop()

    def _before_receive(self):
        """Prepare for receiving data."""
        self._log_data.clear()
        self._start = get_timestamp()

    def _after_receive(self, data):
        """Log data size and time taken for consumption."""
        self._log_data["consume time (ms)"] = calc_datetime(self._start)
        self._log_data["size data received (KB)"] = get_mem_size(data.get("data", data) if isinstance(data, dict) else data)

    def _before_decode(self, _):
        self._before_decode_time = get_timestamp()

    def _after_decode(self, data):
        """Log decoding time and message metadata."""
        self._log_data["decode time (ms)"] = calc_datetime(self._before_decode_time)
        self._log_data["size data decoded (KB)"] = get_mem_size(data)

        if isinstance(self.message, ConsumerRecord):
            now = get_timestamp()
            self._log_data.update({
                "frame delay (ms)": calc_datetime(get_header(self.message.headers, "timestamp"), now),
                "msg creation delay (ms)": calc_datetime(self.message.timestamp / 1e3),
                "offset received": self.message.offset,
                "topic from": self.message.topic
            })
        else:
            self._log_data.update({
                "frame delay (ms)": -1,
                "msg creation delay (ms)": self._log_data["consume time (ms)"],
                "offset received": -1,
                "topic from": None
            })

    def _before_process(self, _):
        self._before_process_time = get_timestamp()

    def _after_process(self, result):
        self._log_data.update({
            "process time (ms)": calc_datetime(self._before_process_time),
            "size data processed (KB)": get_mem_size(result)
        })

    def _before_encode(self, _):
        self._before_encode_time = get_timestamp()

    def _after_encode(self, data):
        self._log_data.update({
            "encode time (ms)": calc_datetime(self._before_encode_time),
            "size data encoded (KB)": get_mem_size(data)
        })

    def _before_send(self, data):
        """Log data size before sending."""
        self._log_data["size data sent (KB)"] = get_mem_size(data)
        self._datetime_before_send = get_timestamp()

    async def _send_logging_data(self, headers, data):
        """Send profiling logs asynchronously."""
        if not self._logging_producer:
            return

        send_data = json.dumps({
            "app_id": self.app_id,
            "log headers": ["timestamp", *headers],
            "log data": [format_timestamp(), *data],
            "extras": getattr(self, "extra_remote_data", {})
        }).encode()

        await self._logging_producer.send(topic=self._logging_topic, value=send_data)

    async def callback(self, record_metadata, *args, log_data=None, headers=None, topic=None, timestamp=None, **kwargs):
        """Callback after sending a message."""
        log_data.update({
            "offset sent": getattr(record_metadata, "offset", -1),
            "frame": int(get_header(headers, "frame", -1)),
            "topic to": topic,
            "send time (ms)": calc_datetime(timestamp)
        })

        headers, res_data = self.finalize_data(log_data)
        await self._send_logging_data(headers, res_data)
        self._log.csv_log(res_data)

    def _after_send(self, data):
        """Finalize logging after sending data."""
        if not self._log_data:
            return
        self._log_data["send time (ms)"] = calc_datetime(self._datetime_before_send)
        headers, res_data = self.finalize_data()
        self._send_logging_data(headers, res_data)
        self._log.csv_log(res_data)
