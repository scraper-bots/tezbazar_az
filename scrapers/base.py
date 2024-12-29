from abc import ABC, abstractmethod
from typing import Dict, List
import requests
import logging
import time
from fake_useragent import UserAgent
from .database import DatabaseManager

logger = logging.getLogger(__name__)

class BaseScraper(ABC):
    def __init__(self, base_url: str, db_manager: DatabaseManager):
        self.base_url = base_url
        self.db_manager = db_manager
        self.user_agent = UserAgent()
        self.session = requests.Session()
    
    def get_headers(self):
        return {
            'User-Agent': self.user_agent.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        }
    
    def make_request(self, url: str) -> requests.Response:
        try:
            response = self.session.get(url, headers=self.get_headers())
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            logger.error(f"Error making request to {url}: {str(e)}")
            raise
    
    @abstractmethod
    def run(self):
        """Main scraping process"""
        pass