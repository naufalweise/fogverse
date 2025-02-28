import logging

from ..logging.formatter import DelimitedFormatter
from ..logging.handler import LogFileRotator
from pathlib import Path

def get_logger(name=None, level=logging.DEBUG, handlers=None, formatter=None):
    """Create and return a logger with the specified configuration that logs messages to the console."""

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(formatter or logging.Formatter(fmt=DEFAULT_FMT))
        handlers = [handler]
    elif not isinstance(handlers, (list, tuple)):
        handlers = [handlers]

    for handler in handlers:
        logger.addHandler(handler)

    return logger

def get_file_logger(name=None, dirname="logs", filename=None, handler=None, formatter=None, mode="w", **kwargs):
    """Create a file-based logger that writes logs to a persistent file."""

    filename = Path(dirname) / (filename or f"log_{name}.txt")
    filename.parent.mkdir(parents=True, exist_ok=True)

    handler = handler or logging.FileHandler(filename, mode=mode)
    handler.setFormatter(formatter or logging.Formatter(fmt=DEFAULT_FMT))

    return get_logger(name, handlers=handler, **kwargs)

def get_csv_logger(name=None, dirname="logs", filename=None, handler=None, mode="w", fmt=None,
                   delimiter=",", datefmt="%Y/%m/%d %H:%M:%S", csv_header=None, header=None, **kwargs):
    """Create a CSV logger."""

    csv_header = csv_header or ["asctime", "name"]
    header = header or []
    fmt = fmt or f"%(asctime)s.%(msecs)03d{delimiter}%(name)s{delimiter}%(message)s"

    filename = Path(dirname) / (filename or f"log_{name}.csv")
    filename.parent.mkdir(parents=True, exist_ok=True)

    handler = handler or LogFileRotator(filename, fmt=fmt, datefmt=datefmt,
                                                header=csv_header + header, delimiter=delimiter, mode=mode)
    handler.setFormatter(DelimitedFormatter(fmt=fmt, datefmt=datefmt, delimiter=delimiter))

    return get_logger(name, handlers=handler, **kwargs)
