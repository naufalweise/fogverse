import json
import time

from aiokafka import ConsumerRecord
from fogverse.logger import FogLogger
from fogverse.producer.kafka import KafkaProducer
from fogverse.profiler.abstract import AbstractProfiler
from fogverse.utils.data import get_header, get_mem_size
from fogverse.utils.time import calc_datetime, get_timestamp, format_timestamp

class FogProfiler(AbstractProfiler):
    DEFAULT_HEADERS = [
        "topic from", "topic to", "frame", "offset received",
        "frame delay (ms)", "msg creation delay (ms)",
        "consume time (ms)", "data size consumed (KB)",
        "decode time (ms)", "data size decoded (KB)",
        "process time (ms)", "data size processed (KB)",
        "encode time (ms)", "data size encoded (KB)",
        "send time (ms)", "data size sent (KB)", "offset sent",
    ]

    def __init__(self, name=f"profiling_{time.time()}", dirname="profiling", log_to_kafka=False, kafka_topic="fogverse-profiling", kafka_servers="localhost"):
        super().__init__()
        self.name = name

        self.log_data = {}
        self._logger = FogLogger(self.name, dirname, self.DEFAULT_HEADERS)

        if log_to_kafka:
            self.kafka_topic = kafka_topic
            self.kafka_servers = kafka_servers

            self._logging_producer = KafkaProducer(
                client_id=self.name,
                producer_server=self.kafka_servers
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
        self._start_time = get_timestamp()

    def _after_receive(self, data):
        """Log data size and time taken for consumption."""
        self._log_data["consume time (ms)"] = calc_datetime(self._start_time)
        self._log_data["data size consumed (KB)"] = get_mem_size(data.get("data", data) if isinstance(data, dict) else data)

    def _before_decode(self, _):
        self._before_decode_time = get_timestamp()

    def _after_decode(self, data):
        """Log decoding time and message metadata."""
        self._log_data["decode time (ms)"] = calc_datetime(self._before_decode_time)
        self._log_data["data size decoded (KB)"] = get_mem_size(data)

        if isinstance(self.message, ConsumerRecord):
            now = get_timestamp()
            self._log_data.update({
                "frame delay (ms)": calc_datetime(get_header(self.message.headers, "timestamp"), now),
                "msg creation delay (ms)": calc_datetime(self.message.timestamp / 1_000),
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
            "data size processed (KB)": get_mem_size(result)
        })

    def _before_encode(self, _):
        self._before_encode_time = get_timestamp()

    def _after_encode(self, data):
        self._log_data.update({
            "encode time (ms)": calc_datetime(self._before_encode_time),
            "data size encoded (KB)": get_mem_size(data)
        })

    def _before_send(self, data):
        """Log data size before sending."""
        self._log_data["data size sent (KB)"] = get_mem_size(data)
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

    def finalize_data(self, log_records=None, missing_value=None):
        """Prepare final log data."""

        # Use provided log data or fallback to the instance's stored log data.
        log_records = log_records or self.log_records

        # Return a header row and a list of corresponding values.
        return self._df_header, [log_records.get(header, missing_value) for header in self._df_header]
