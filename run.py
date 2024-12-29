from scrapers.database import DatabaseManager
from scrapers.autonet import AutonetScraper
from dotenv import load_dotenv
import os
import logging
import psycopg2.extras
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def main():
    try:
        start_time = time.time()
        
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
        
        # Initialize and run Autonet scraper
        logger.info("Starting Autonet scraper...")
        scraper = AutonetScraper('https://autonet.az', db_manager)
        scraper.run()
        
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Scraping completed in {duration:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        raise

if __name__ == "__main__":
    main()