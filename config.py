# config.py
from dataclasses import dataclass
from typing import Dict

@dataclass
class SiteConfig:
    name: str
    url: str
    table_name: str

# Database configuration
DB_CONFIG = {
    "host": "ep-white-cloud-a2453ie4.eu-central-1.aws.neon.tech",
    "port": 5432,
    "user": "neondb_owner",
    "password": "gocazMi82pXl",
    "database": "neondb"
}

# Autonet configuration
AUTONET_CONFIG = SiteConfig(
    name="autonet",
    url="https://autonet.az/api/items/searchItem",
    table_name="autonet_cars"
)