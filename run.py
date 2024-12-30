from scrapers.database import DatabaseManager
from scrapers.autonet import AutonetScraper
from scrapers.arenda import ArendaScraper
from scrapers.birjain import BirjaInScraper
from dotenv import load_dotenv
import os
import logging
import psycopg2.extras
import time
from typing import List, Type, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def configure_scraper(scraper_instance, max_pages):
    """Configure scraper with maximum pages if supported"""
    if hasattr(scraper_instance, 'set_max_pages'):
        scraper_instance.set_max_pages(max_pages)
        logger.info(f"Configured {scraper_instance.__class__.__name__} to scrape {max_pages} pages")

def get_scrapers() -> List[Tuple[Type, str, int]]:
    """
    Returns a list of tuples containing:
    - Scraper class
    - Base URL
    - Maximum pages to scrape
    """
    return [
        (AutonetScraper, 'https://autonet.az', 2),  # Scrape 2 pages from autonet
        (ArendaScraper, 'https://arenda.az', 2),    # Scrape 2 pages from arenda
        (BirjaInScraper, 'https://birja-in.az', 3)  # Scrape 3 pages from birja-in
    ]

def run_scraper(scraper_class, base_url: str, max_pages: int, db_manager: DatabaseManager) -> float:
    """Run a single scraper and return the execution time"""
    start_time = time.time()
    
    try:
        logger.info(f"Starting {scraper_class.__name__}...")
        scraper = scraper_class(base_url, db_manager)
        configure_scraper(scraper, max_pages)
        scraper.run()
        
    except Exception as e:
        logger.error(f"Error running {scraper_class.__name__}: {str(e)}")
        raise
    finally:
        duration = time.time() - start_time
        logger.info(f"{scraper_class.__name__} completed in {duration:.2f} seconds")
        return duration

def main():
    total_start_time = time.time()
    scraper_times = {}
    
    try:
        # Load environment variables
        load_dotenv()
        
        # Database configuration
        db_config = {
            'dbname': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT')
        }
        
        # Initialize database manager
        logger.info("Initializing database connection...")
        db_manager = DatabaseManager(db_config)
        
        # Run each scraper sequentially with its configured page limit
        for scraper_class, base_url, max_pages in get_scrapers():
            try:
                duration = run_scraper(scraper_class, base_url, max_pages, db_manager)
                scraper_times[scraper_class.__name__] = duration
            except Exception as e:
                logger.error(f"Scraper {scraper_class.__name__} failed: {str(e)}")
                continue
        
        # Log summary
        total_duration = time.time() - total_start_time
        logger.info("\nScraping Summary:")
        logger.info("-" * 40)
        for scraper_name, duration in scraper_times.items():
            logger.info(f"{scraper_name}: {duration:.2f} seconds")
        logger.info("-" * 40)
        logger.info(f"Total execution time: {total_duration:.2f} seconds\n")
        
    except Exception as e:
        logger.error(f"Critical error in main process: {str(e)}")
        raise

if __name__ == "__main__":
    main()