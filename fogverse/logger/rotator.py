from fogverse.logger.formatter import DelimitedFormatter
from logging.handlers import RotatingFileHandler
from os import path

class LogFileRotator(RotatingFileHandler):
    """Rotating file handler for CSV logging."""

    def __init__(self, filename, fmt=None, datefmt=None, max_size=0, header=None, delimiter=",", mode="a", **kwargs):
        super().__init__(filename, maxBytes=max_size, mode=mode, **kwargs)

        # Format the header into a CSV string if one is provided.
        self.formatter = DelimitedFormatter(fmt, datefmt, delimiter)
        self._header = self.formatter.delimit_message(header) if header else None

        # Write header if the file is new or opened in write mode.
        if self.stream and (mode == "w" or not path.exists(filename)) and self._header:
            self.stream.write(self._header + "\n")

    def doRollover(self):
        """Perform rollover and prepend the CSV header to the new log file."""

        super().doRollover()

        if not self._header or not self.formatter:
            return
        else:
            # NOTE: Temporarily disable formatter to avoid extra processing (legacy behavior).
            original_format = self.formatter.format
            self.formatter.format = lambda x: x
            self.handle(self._header)  # Write header.
            self.formatter.format = original_format
