import logging
import os
from datetime import datetime
from pathlib import Path
from logging.handlers import RotatingFileHandler

def setup_logger():
    """Setup logging configuration with file size rotation"""
    # Create logs directory if it doesn't exist
    log_dir = Path(__file__).parent.parent / 'logs'
    log_dir.mkdir(exist_ok=True)
    print("setup_logger: Logs directory is set up at", log_dir)
    # Create log filename with current date
    current_date = datetime.now().strftime('%Y%m%d')
    log_file = log_dir / f'jjm_logs_{current_date}.log'

    # Setup logging configuration
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            # Rotating file handler for file size-based rotation
            RotatingFileHandler(
                log_file,
                maxBytes=10 * 1024 * 1024,  # 10MB
                backupCount=5,  # Keep 5 old log files
                encoding='utf-8'
            ),
            # Console handler for immediate feedback
            logging.StreamHandler()
        ]
    )

    # Get the root logger
    logger = logging.getLogger()
    return logger
