import logging

from .formatter import DelimitedFormatter
from .handler import LogFileRotator
from pathlib import Path

# Define custom log levels as module-level constants.
FOGV_STDOUT = logging.INFO + 2
FOGV_CSV = logging.INFO + 4
FOGV_FILE = logging.INFO + 8

# Register custom log levels with logging.
logging.addLevelName(FOGV_STDOUT, "FOGV_STDOUT")
logging.addLevelName(FOGV_CSV, "FOGV_CSV")
logging.addLevelName(FOGV_FILE, "FOGV_FILE")

DEFAULT_FMT = '[%(asctime)s][%(levelname)s][%(name)s] %(message)s'

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

class FogVerseLogging:
    """Centralized logging for FogVerse, supporting standard, file, and CSV logs."""

    def __init__(self, name=None, dirname="logs", csv_header=None, level=FOGV_STDOUT,
                 std_log_kwargs=None, csv_log_kwargs=None, file_log_kwargs=None):
        std_log_kwargs, csv_log_kwargs, file_log_kwargs = std_log_kwargs or {}, csv_log_kwargs or {}, file_log_kwargs or {}

        self._std_log = get_logger(f"std_{name}", level, **std_log_kwargs)
        self._file_log = get_file_logger(f"file_{name}", dirname, level=level, **file_log_kwargs)
        self._csv_log = get_csv_logger(f"csv_{name}", dirname, header=csv_header or [], level=level, **csv_log_kwargs)

    def setLevel(self, level):
        for logger in [self._std_log, self._file_log, self._csv_log]:
            logger.setLevel(level)

    def _log(self, logger, level, message, *args, **kwargs):
        if logger.isEnabledFor(level):
            logger._log(level, message, args, **kwargs)

    def std_log(self, message, *args, **kwargs):
        self._log(self._std_log, FOGV_STDOUT, message, *args, **kwargs)
        self._log(self._file_log, FOGV_FILE, message, *args, **kwargs)

    def csv_log(self, message, *args, **kwargs):
        self._log(self._csv_log, FOGV_CSV, message, *args, **kwargs)

    def file_log(self, message, *args, **kwargs):
        self._log(self._file_log, FOGV_FILE, message, *args, **kwargs)
