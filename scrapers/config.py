import os
from typing import Dict

# Database settings
def get_db_config() -> Dict:
    return {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT')
    }

# Scraper settings
SCRAPER_SETTINGS = {
    'max_retries': 3,
    'retry_delay': 5,
    'request_delay': 2,
    'max_pages': 3
}

# Logging settings
LOG_CONFIG = {
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'level': 'INFO',
    'filename': 'scraper.log',
    'maxBytes': 10*1024*1024,  # 10MB
    'backupCount': 5
}