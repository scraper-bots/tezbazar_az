from scrapers.database import DatabaseManager
from scrapers.autonet import AutonetScraper
from scrapers.arenda import ArendaScraper
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

def get_scrapers() -> List[Tuple[Type, str]]:
    """
    Returns a list of scraper classes and their base URLs.
    Add new scrapers here to include them in the main process.
    """
    return [
        (AutonetScraper, 'https://autonet.az'),
        (ArendaScraper, 'https://arenda.az')
    ]

def run_scraper(scraper_class, base_url: str, db_manager: DatabaseManager) -> float:
    """Run a single scraper and return the execution time"""
    start_time = time.time()
    
    try:
        logger.info(f"Starting {scraper_class.__name__}...")
        scraper = scraper_class(base_url, db_manager)
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
        
        # Run each scraper sequentially
        for scraper_class, base_url in get_scrapers():
            try:
                duration = run_scraper(scraper_class, base_url, db_manager)
                scraper_times[scraper_class.__name__] = duration
            except Exception as e:
                logger.error(f"Scraper {scraper_class.__name__} failed: {str(e)}")
                # Continue with next scraper even if one fails
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