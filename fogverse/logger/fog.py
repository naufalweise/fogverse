from fogverse.constants import FOGV_CSV, FOGV_TXT, FOGV_STDOUT
from fogverse.utils.logger import get_base_logger, get_txt_logger, get_csv_logger

import logging
import time

# Register custom log levels with `logging`.
logging.addLevelName(FOGV_STDOUT, "FOGV_STDOUT")
logging.addLevelName(FOGV_TXT, "FOGV_TXT")
logging.addLevelName(FOGV_CSV, "FOGV_CSV")

class FogLogger:
    """Centralized logging for FogVerse, supporting standard, txt, and csv logs."""

    def __init__(self, name=None, dirname="logs", csv_header=[], std_log_kwargs={}, txt_log_kwargs={}, csv_log_kwargs={}):
        super().__init__()

        self._std_log = get_base_logger(name, **std_log_kwargs)
        self._txt_log = get_txt_logger(name, dirname, **txt_log_kwargs)
        self._csv_log = get_csv_logger(name, dirname, csv_header, **csv_log_kwargs)

    def _log(self, logger, message, *args, **kwargs):
        logger._log(logger.level, message, args, **kwargs)

    def std_log(self, message, *args, **kwargs):
        self._log(self._std_log, message, *args, **kwargs)

    def txt_log(self, message, *args, **kwargs):
        self._log(self._txt_log, message, *args, **kwargs)

    def csv_log(self, message, *args, **kwargs):
        self._log(self._csv_log, message, *args, **kwargs)
