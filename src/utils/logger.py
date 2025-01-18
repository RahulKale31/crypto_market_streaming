import logging
import sys
from logging.handlers import RotatingFileHandler
import os

class Logger:
    def __init__(self, name, log_file=None):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # Create file handler if log_file is specified
        if log_file:
            # Create logs directory if it doesn't exist
            os.makedirs('logs', exist_ok=True)
            file_handler = RotatingFileHandler(
                f'logs/{log_file}',
                maxBytes=10485760,  # 10MB
                backupCount=5
            )
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
    
    def get_logger(self):
        return self.logger