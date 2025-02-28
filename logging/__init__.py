import logging

from utils.logger import get_csv_logger, get_file_logger, get_logger

# Define custom log levels as module-level constants.
FOGV_STDOUT = logging.INFO + 2
FOGV_CSV = logging.INFO + 4
FOGV_FILE = logging.INFO + 8

# Register custom log levels with logging.
logging.addLevelName(FOGV_STDOUT, "FOGV_STDOUT")
logging.addLevelName(FOGV_CSV, "FOGV_CSV")
logging.addLevelName(FOGV_FILE, "FOGV_FILE")

DEFAULT_FMT = '[%(asctime)s][%(levelname)s][%(name)s] %(message)s'

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
