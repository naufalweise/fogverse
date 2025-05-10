from logging import Formatter

class DelimitedFormatter(Formatter):
    """Custom CSV formatter for logging, ensuring messages are formatted as CSV-friendly strings."""

    def __init__(self, message_format=None, datefmt=None, delimiter=","):
        super().__init__(message_format, datefmt)
        self.delimiter = delimiter

    def delimit_message(self, msg):
        """Convert list-like messages to a CSV string while leaving strings unchanged."""
        return self.delimiter.join(map(str, msg)) if isinstance(msg, (list, tuple)) else str(msg)

    def format(self, record):
        """Ensure the log message is properly formatted before passing to the base Formatter."""
        record.msg = self.delimit_message(record.msg)
        return super().format(record)
