from fogverse.constants import DEFAULT_FMT
from fogverse.logger.formatter import DelimitedFormatter
from fogverse.logger.handler import LogFileRotator
from pathlib import Path

import logging

def get_base_logger(name=None, level=logging.DEBUG, handlers=None, formatter=None):
    """Create and configure a logger that outputs messages to the console or specified handlers."""

    # Get or create a logger with the given name.
    logger = logging.getLogger(name)

    # Set the logging level (default is DEBUG).
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

def get_txt_logger(name=None, dirname="logs", filename=None, mode="w", **kwargs):
    """Create a txt-based logger that writes logs to a persistent file."""

    # Determine the full file path where logs will be stored.
    filename = Path(dirname) / (filename or f"log_{name}.txt")

    # Ensure the directory exists before writing logs.
    filename.parent.mkdir(parents=True, exist_ok=True)

    # Create a file handler.
    handler = logging.FileHandler(filename, mode=mode)

    # Set the log message format.
    handler.setFormatter(logging.Formatter(fmt=DEFAULT_FMT))

    # Create and return a logger using the base logger function, with the file handler attached.
    return get_base_logger(name, handlers=handler, **kwargs)

def get_csv_logger(name=None, dirname="logs", filename=None, mode="w", delimiter=",", datefmt="%Y/%m/%d %H:%M:%S", header=[], **kwargs):
    """Create a CSV-based logger that writes logs to a persistent file."""

    # Define the log message format using the specified delimiter.
    fmt = f"%(asctime)s.%(msecs)03d{delimiter}%(name)s{delimiter}%(message)s"

    # Determine the full file path where logs will be stored.
    filename = Path(dirname) / (filename or f"log_{name}.csv")

    # Ensure the directory exists before writing logs.
    filename.parent.mkdir(parents=True, exist_ok=True)

    # Create a rotating file handler.
    handler = LogFileRotator(filename, fmt=fmt, datefmt=datefmt, header=header, delimiter=delimiter, mode=mode)

    # Set the log message format using a custom CSV-friendly formatter.
    handler.setFormatter(DelimitedFormatter(fmt=fmt, datefmt=datefmt, delimiter=delimiter))

    # Create and return a logger using the base logger function, with the rotating file handler attached.
    return get_base_logger(name, handlers=handler, **kwargs)
