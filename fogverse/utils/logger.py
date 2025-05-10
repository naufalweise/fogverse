import logging
import time

from fogverse.constants import DEFAULT_FMT, FOGV_STDOUT, FOGV_TXT, FOGV_CSV
from fogverse.logger.formatter import DelimitedFormatter
from fogverse.logger.rotator import LogFileRotator
from pathlib import Path

def get_base_logger(name=None, level=FOGV_STDOUT, handlers=None, formatter=None):
    """Create and configure a logger that outputs messages to the console or specified handlers."""

    # Get or create a logger with the given name.
    logger = logging.getLogger(name)

    # Set the logging level.
    logger.setLevel(level)

    # If no handlers are provided, create a default StreamHandler (console output).
    if not handlers:
        handler = logging.StreamHandler()  # Create a console handler.
        handler.setFormatter(formatter or logging.Formatter(fmt=DEFAULT_FMT))  # Set formatter (default if none provided).
        handlers = [handler]  # Wrap in a list for consistency.

    # Ensure handlers is always a list, even if a single handler is passed.
    elif not isinstance(handlers, (list, tuple)):
        handlers = [handlers]

    # Attach each handler to the logger.
    for handler in handlers:
        logger.addHandler(handler)

    return logger  # Return the configured logger.

def get_txt_logger(name=None, dirname="logs", mode="w", **kwargs):
    """Create a txt-based logger that writes logs to a persistent file."""

    # Determine the full file path where logs will be stored.
    filename = Path(dirname) / (name or f"log_{int(time.time())}.txt")

    # Ensure the directory exists before writing logs.
    filename.parent.mkdir(parents=True, exist_ok=True)

    # Create a file handler.
    handler = logging.FileHandler(filename, mode=mode)

    # Set the log message format.
    handler.setFormatter(logging.Formatter(fmt=DEFAULT_FMT))

    # Create and return a logger using the base logger function, with the file handler attached.
    return get_base_logger(name, level=FOGV_TXT, handlers=handler, **kwargs)

def get_csv_logger(name=None, dirname="logs", header=[], mode="w", delimiter=",", datefmt="%Y/%m/%d %H:%M:%S", **kwargs):
    """Create a CSV-based logger that writes logs to a persistent file."""

    # Define the log message format using the specified delimiter.
    message_format = f"%(asctime)s.%(msecs)03d{delimiter}%(name)s{delimiter}%(message)s"

    # Determine the full file path where logs will be stored.
    filename = Path(dirname) / (name or f"log_{int(time.time())}.csv")

    # Ensure the directory exists before writing logs.
    filename.parent.mkdir(parents=True, exist_ok=True)

    # Create a rotating file handler.
    handler = LogFileRotator(filename, message_format=message_format, datefmt=datefmt, header=header, delimiter=delimiter, mode=mode)

    # Set the log message format using a custom CSV-friendly formatter.
    handler.setFormatter(DelimitedFormatter(message_format=message_format, datefmt=datefmt, delimiter=delimiter))

    # Create and return a logger using the base logger function, with the rotating file handler attached.
    return get_base_logger(name, level=FOGV_CSV, handlers=handler, **kwargs)
