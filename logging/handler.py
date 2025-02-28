import logging

from .formatter import DelimitedFormatter
from logging.handlers import RotatingFileHandler
from os import path

class LogFileRotator(RotatingFileHandler):
    """Rotating file handler for CSV logging, ensuring headers persist across rollovers."""

    def __init__(self, filename, fmt=None, datefmt=None, max_size=0, header=None, delimiter=',', mode='a', **kwargs):
        self.file_exists = path.exists(filename)  # Check if file exists before writing
        super().__init__(filename, maxBytes=max_size, mode=mode, **kwargs)

        self.formatter = DelimitedFormatter(fmt, datefmt, delimiter)
        self._header = self.formatter.delimit_message(header) if header else None

        # Write header if the file is new or opened in write mode
        if self.stream and (mode == 'w' or not self.file_exists) and self._header:
            self.stream.write(self._header + '\n')

    def doRollover(self):
        """Perform rollover and prepend the CSV header to the new log file."""
        super().doRollover()
        if not self._header or not self.formatter:
            return

        # Create a dummy LogRecord with the header message.
        header_record = logging.LogRecord(
            name="CSV_Header",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=self._header,
            args=(),
            exc_info=None,
        )

        # Temporarily override the formatter's format method to bypass extra formatting.
        original_format = self.formatter.format

        def bypass_format(record: logging.LogRecord) -> str:
            return record.msg

        self.formatter.format = bypass_format # Override with our function.
        self.handle(header_record) # Write the header.
        self.formatter.format = original_format # Restore the original format method.
