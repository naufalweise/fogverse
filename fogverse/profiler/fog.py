import json
import time

from aiokafka import ConsumerRecord
from fogverse.logger import FogLogger
from fogverse.producer.kafka import KafkaProducer
from fogverse.profiler.abstract import AbstractProfiler
from fogverse.runnable import Runnable
from fogverse.utils.data import get_header, get_mem_size
from fogverse.utils.time import calc_datetime, format_timestamp, get_timestamp

class FogProfiler(AbstractProfiler):
    """Profiler for tracking and logging various performance metrics of a component."""

    DEFAULT_HEADERS = [
        "topic from", "topic to", "frame", "offset received",
        "frame delay (ms)", "msg creation delay (ms)",
        "consume time (ms)", "data size consumed (KB)",
        "decode time (ms)", "data size decoded (KB)",
        "process time (ms)", "data size processed (KB)",
        "encode time (ms)", "data size encoded (KB)",
        "send time (ms)", "data size sent (KB)", "offset sent",
    ]

    def __init__(self, component: Runnable, name=f"profiling_{time.time()}", dirname="profiling", topic="fogverse-profiling"):
        super().__init__()
        self.name = name

        self.log_data = {}
        self._logger = FogLogger(self.name, dirname, self.DEFAULT_HEADERS)

        self.component = component
        self.topic = topic  # This must not be None if `component` is a `KafkaProducer`.

    def run(self): self.component.run()
    def stop(self): self.component.stop()

    def _before_receive(self):
        """Clear log data and store the start time before receiving data."""

        self.log_data.clear()
        self._start_time = get_timestamp()

    def _after_receive(self, data):
        """Log data after receiving, including time taken and data size."""

        self.log_data["consume time (ms)"] = calc_datetime(self._start_time)
        self.log_data["data size consumed (KB)"] = get_mem_size(data.get("data", data) if isinstance(data, dict) else data)

    def _before_decode(self, _):
        """Record the timestamp before decoding data."""

        self._before_decode_time = get_timestamp()

    def _after_decode(self, data):
        """Log time taken to decode and store relevant metadata."""

        self.log_data["decode time (ms)"] = calc_datetime(self._before_decode_time)
        self.log_data["data size decoded (KB)"] = get_mem_size(data)

        if isinstance(data, ConsumerRecord):
            now = get_timestamp()
            self.log_data.update({
                "frame delay (ms)": calc_datetime(get_header(data.headers, "timestamp"), now),
                "msg creation delay (ms)": calc_datetime(data.timestamp / 1_000),
                "offset received": data.offset,
                "topic from": data.topic
            })
        else:
            self.log_data.update({
                "frame delay (ms)": -1,
                "msg creation delay (ms)": self.log_data["consume time (ms)"],
                "offset received": -1,
                "topic from": None
            })

    def _before_process(self, _):
        """Record the timestamp before processing data."""

        self._before_process_time = get_timestamp()

    def _after_process(self, result):
        """Log time taken to process and store processed data size."""

        self.log_data["process time (ms)"] = calc_datetime(self._before_process_time)
        self.log_data["data size processed (KB)"] = get_mem_size(result)

    def _before_encode(self, _):
        """Record the timestamp before encoding data."""

        self._before_encode_time = get_timestamp()

    def _after_encode(self, data):
        """Log time taken to encode and store encoded data size."""

        self.log_data["encode time (ms)"] =calc_datetime(self._before_encode_time)
        self.log_data["data size encoded (KB)"] = get_mem_size(data)

    def _before_send(self, data):
        """Record the timestamp before sending data and log data size."""

        self._datetime_before_send = get_timestamp()
        self.log_data["data size sent (KB)"] = get_mem_size(data)

    def _after_send(self, _):
        """Log the time taken to send data and save log if profiling data exists."""

        if not self.log_data: return

        self.log_data["send time (ms)"] = calc_datetime(self._datetime_before_send)
        _, log_data = self.finalize_data()
        self._logger.csv_log(log_data)

    def finalize_data(self, log_data=None, default_value=None):
        """Format log data by aligning with headers and returning structured data."""

        return self._df_header, [log_data.get(header, default_value) for header in self._df_header]

    async def _send_kafka_profiling_data(self, headers, log_data):
        """Send profiling data to Kafka for remote analysis."""

        if not isinstance(self.component, KafkaProducer): return

        send_data = json.dumps({
            "profiler": self.name,
            "log headers": ["timestamp", *headers],
            "log data": [format_timestamp(), *log_data],
            "extras": getattr(self, "extra_remote_data", {})
        }).encode()

        await self.component.send(topic=self.topic, value=send_data)
