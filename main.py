import asyncio
import os
import sys
import logging
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, List, Any
from scrapers import arenda, autonet, birja, birjain, boss, emlak, ipoteka, qarabazar, ucuztap
# from scrapers import ucuztap  # Import other scrapers as needed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'scraper_{datetime.now().strftime("%Y%m%d")}.log')
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class DatabaseManager:
    """Database connection manager with context support"""
    def __init__(self):
        self.conn = None
        self.required_env_vars = ['DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT']

    def __enter__(self):
        self.conn = self.get_connection()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            try:
                self.conn.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")

    def get_connection(self) -> psycopg2.extensions.connection:
        """Create and return database connection"""
        # Check for required environment variables
        missing_vars = [var for var in self.required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        try:
            return psycopg2.connect(
                dbname=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT')
            )
        except psycopg2.Error as e:
            logger.error(f"Database connection error: {e}")
            raise

async def run_scraper(scraper_module) -> None:
    """Run a scraper in a thread pool and save results to database"""
    loop = asyncio.get_event_loop()
    scraper_name = scraper_module.__name__.split('.')[-1]
    
    logger.info(f"Starting {scraper_name} scraper")
    
    with ThreadPoolExecutor() as pool:
        try:
            start_time = datetime.now()
            
            # Run the scraper
            data = await loop.run_in_executor(pool, scraper_module.scrape)
            
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"Completed scraping with {scraper_name}, found {len(data) if data else 0} items in {duration:.2f} seconds")
            
            # Save data if we have any
            if data:
                try:
                    # Let the scraper manage its own database connection
                    if hasattr(scraper_module, 'save_to_db'):
                        await loop.run_in_executor(pool, lambda: scraper_module.save_to_db(data))
                        logger.info(f"Saved data from {scraper_name} to database")
                except Exception as e:
                    logger.error(f"Database error in {scraper_name}: {e}")
            else:
                logger.warning(f"No data returned from {scraper_name}")
                
        except Exception as e:
            logger.error(f"Error in {scraper_name}: {e}", exc_info=True)
            
        finally:
            logger.info(f"Finished processing {scraper_name}")

async def main() -> None:
    """Main function to run all scrapers concurrently"""
    scrapers = [
        arenda,
        autonet,
        birja,
        birjain, 
        boss,
        emlak,
        ipoteka,
        qarabazar,
        ucuztap
    ]
    
    try:
        # Create tasks for all scrapers
        tasks = [run_scraper(scraper) for scraper in scrapers]
        
        # Run all scrapers concurrently
        start_time = datetime.now()
        await asyncio.gather(*tasks)
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"All scrapers completed in {duration:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        raise

def run():
    """Entry point function with error handling"""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    run()