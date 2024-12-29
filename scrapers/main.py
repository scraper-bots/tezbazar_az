from dotenv import load_dotenv
import logging
from logging.handlers import RotatingFileHandler
import sys
import time
from datetime import datetime

from .config import get_db_config, LOG_CONFIG, SCRAPER_SETTINGS
from .database import DatabaseManager
from .autonet import AutonetScraper

# Configure logging
def setup_logging():
    handlers = []
    
    # File handler
    file_handler = RotatingFileHandler(
        LOG_CONFIG['filename'],
        maxBytes=LOG_CONFIG['maxBytes'],
        backupCount=LOG_CONFIG['backupCount']
    )
    file_handler.setFormatter(logging.Formatter(LOG_CONFIG['format']))
    handlers.append(file_handler)
    
    # Console handler
    console_handler = logging.