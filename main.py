import asyncio
import logging
from datetime import datetime
import importlib
import os
from typing import List, Dict, Any
import sys
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class ScraperManager:
    def __init__(self, db_connection_string: str):
        self.db_connection_string = db_connection_string
        self.scrapers = []
        self.load_scrapers()
        
    def load_scrapers(self):
        """Dynamically load all scraper modules from the scrapers directory"""
        scraper_dir = "scrapers"
        for file in os.listdir(scraper_dir):
            if file.endswith(".py") and file != "__init__.py":
                module_name = f"scrapers.{file[:-3]}"
                try:
                    module = importlib.import_module(module_name)
                    if hasattr(module, 'Scraper'):
                        self.scrapers.append(module.Scraper())
                        logger.info(f"Loaded scraper: {module_name}")
                except Exception as e:
                    logger.error(f"Failed to load scraper {module_name}: {str(e)}")

    async def run_scraper(self, scraper) -> List[Dict[Any, Any]]:
        """Run a single scraper and return the results"""
        try:
            logger.info(f"Starting scraper: {scraper.__class__.__name__}")
            data = await scraper.scrape()
            logger.info(f"Completed scraper: {scraper.__class__.__name__}, Items: {len(data)}")
            return data
        except Exception as e:
            logger.error(f"Error in scraper {scraper.__class__.__name__}: {str(e)}")
            return []

    async def save_to_db(self, data: List[Dict[Any, Any]], source: str):
        """Save scraped data to database"""
        try:
            # Implement your database saving logic here
            # Example using asyncpg for PostgreSQL:
            # async with asyncpg.create_pool(self.db_connection_string) as pool:
            #     async with pool.acquire() as connection:
            #         for item in data:
            #             await connection.execute(
            #                 'INSERT INTO listings (source, data, created_at) VALUES ($1, $2, $3)',
            #                 source, item, datetime.now()
            #             )
            logger.info(f"Saved {len(data)} items from {source} to database")
        except Exception as e:
            logger.error(f"Database error for {source}: {str(e)}")

    async def run_all_scrapers(self):
        """Run all scrapers concurrently"""
        tasks = []
        for scraper in self.scrapers:
            tasks.append(self.run_scraper(scraper))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Save results to database
        save_tasks = []
        for scraper, data in zip(self.scrapers, results):
            if isinstance(data, list):  # Check if scraping was successful
                save_tasks.append(self.save_to_db(data, scraper.__class__.__name__))
        
        if save_tasks:
            await asyncio.gather(*save_tasks)

    async def start_scheduling(self):
        """Start the scheduling loop"""
        while True:
            start_time = datetime.now()
            logger.info("Starting scraping cycle")
            
            await self.run_all_scrapers()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.info(f"Completed scraping cycle in {duration} seconds")
            
            # Wait for the next 2-hour interval
            await asyncio.sleep(7200 - duration if duration < 7200 else 0)

if __name__ == "__main__":
    # Replace with your database connection string
    DB_CONNECTION_STRING = "postgresql://user:password@localhost:5432/database"
    
    manager = ScraperManager(DB_CONNECTION_STRING)
    
    try:
        asyncio.run(manager.start_scheduling())
    except KeyboardInterrupt:
        logger.info("Shutting down scrapers")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")