# main.py
import asyncio
import os
import sys
import logging
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from concurrent.futures import ThreadPoolExecutor
# from scrapers import arenda, autonet, birja, birjain, boss, emlak, ipoteka
from scrapers import ipoteka

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

def get_db_connection():
    """Create and return database connection using environment variables"""
    required_env_vars = ['DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT']
    
    # Check for required environment variables
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
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

async def run_scraper(scraper_module, db_connection):
    """Run a scraper in a thread pool and save results to database"""
    loop = asyncio.get_event_loop()
    scraper_name = scraper_module.__name__.split('.')[-1]
    
    logger.info(f"Starting {scraper_name} scraper")
    
    with ThreadPoolExecutor() as pool:
        try:
            start_time = datetime.now()
            
            # Handle both returned data and stats
            result = await loop.run_in_executor(pool, scraper_module.scrape)
            if isinstance(result, tuple):
                data, stats = result
            else:
                data = result
                stats = None  # For backwards compatibility with other scrapers
            
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"Completed scraping with {scraper_name}, found {len(data) if data else 0} items in {duration:.2f} seconds")
            
            if data and hasattr(scraper_module, 'save_to_db'):
                try:
                    # Pass stats if available
                    if stats:
                        await loop.run_in_executor(pool, scraper_module.save_to_db, db_connection, data, stats)
                    else:
                        await loop.run_in_executor(pool, scraper_module.save_to_db, db_connection, data)
                    logger.info(f"Saved data from {scraper_name} to database")
                except Exception as e:
                    logger.error(f"Database error in {scraper_name}: {e}")
            elif not data:
                logger.warning(f"No data returned from {scraper_name}")
            elif not hasattr(scraper_module, 'save_to_db'):
                logger.warning(f"No save_to_db function found in {scraper_name}")
            
        except Exception as e:
            logger.error(f"Error in {scraper_name}: {e}", exc_info=True)
            
        finally:
            logger.info(f"Finished processing {scraper_name}")
            
async def main():
    """Main function to run all scrapers concurrently"""
    # List of all scraper modules
    scrapers = [
        # arenda,
        # autonet,
        # birja,
        # birjain, 
        # boss,
        # emlak,
        ipoteka
    ]
    
    conn = None
    try:
        # Create database connection
        conn = get_db_connection()
        logger.info("Successfully connected to database")
        
        # Create tasks for all scrapers
        tasks = [run_scraper(scraper, conn) for scraper in scrapers]
        
        # Run all scrapers concurrently
        start_time = datetime.now()
        await asyncio.gather(*tasks)
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"All scrapers completed in {duration:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        raise
    
    finally:
        # Close database connection
        if conn:
            try:
                conn.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)