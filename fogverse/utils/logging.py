import logging
import sys

class FogLogger:
    """Unified logging handler with configurable outputs."""
    
    def __init__(self, name: str, level=logging.INFO, log_file: str = None):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        console = logging.StreamHandler(sys.stdout)
        console.setFormatter(formatter)
        self.logger.addHandler(console)
        
        if log_file:
            file = logging.FileHandler(log_file)
            file.setFormatter(formatter)
            self.logger.addHandler(file)
    
    def info(self, message: str, **kwargs):
        self.logger.info(message, extra=kwargs)
    
    def warning(self, message: str, **kwargs):
        self.logger.warning(message, extra=kwargs)
    
    def error(self, message: str, **kwargs):
        self.logger.error(message, extra=kwargs)
