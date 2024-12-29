# config.py
import os
from dataclasses import dataclass
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

@dataclass
class SiteConfig:
    name: str
    url: str
    table_name: str

# Database configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME")
}

# Autonet configuration
AUTONET_CONFIG = SiteConfig(
    name="autonet",
    url="https://autonet.az/api/items/searchItem",
    table_name="autonet_az"
)