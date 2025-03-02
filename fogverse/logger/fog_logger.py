import logging

from ..constants import FOGV_CSV, FOGV_FILE, FOGV_STDOUT
from ..utils.logger import get_csv_logger, get_file_logger, get_base_logger

# Register custom log levels with logging.
logging.addLevelName(FOGV_STDOUT, "FOGV_STDOUT")
logging.addLevelName(FOGV_CSV, "FOGV_CSV")
logging.addLevelName(FOGV_FILE, "FOGV_FILE")

class FogLogger:
    """Centralized logging for FogVerse, supporting standard, file, and CSV logs."""

    def __init__(
        self, name=None,
        dirname="logs",
        csv_header=[],
        level=FOGV_STDOUT,
        std_log_kwargs={},
        csv_log_kwargs={},
        file_log_kwargs={}
    ):
        super().__init__()

        std_log_kwargs, csv_log_kwargs, file_log_kwargs = std_log_kwargs, csv_log_kwargs, file_log_kwargs

        self._std_log = get_base_logger(f"std_{name}", level, **std_log_kwargs)
        self._file_log = get_file_logger(f"file_{name}", dirname, level=level, **file_log_kwargs)
        self._csv_log = get_csv_logger(f"csv_{name}", dirname, header=csv_header, level=level, **csv_log_kwargs)

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
