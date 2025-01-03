import asyncio
import os
import sys
import json
import logging
import signal
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, List, Any, NoReturn
from contextlib import contextmanager
from scrapers import yeniemlak

# Configure logging with rotation
from logging.handlers import RotatingFileHandler

log_file = f'scraper_{datetime.now():%Y%m%d}.log'
handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[handler]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Signal handling for graceful shutdown
def signal_handler(signum, frame) -> NoReturn:
    logger.warning(f"Received signal {signum}. Initiating graceful shutdown...")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

class DatabaseManager:
    def __init__(self):
        self.pool = None
        self.required_env_vars = ['DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT']
        self.setup_pool()

    def setup_pool(self) -> None:
        missing_vars = [var for var in self.required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise EnvironmentError(f"Missing environment variables: {', '.join(missing_vars)}")
        
        try:
            self.pool = ThreadedConnectionPool(
                minconn=5,
                maxconn=20,
                dbname=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT'),
                cursor_factory=RealDictCursor
            )
        except Exception as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise

    def __enter__(self):
        self._conn = self.pool.getconn()
        return self._conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.pool:
            try:
                if hasattr(self, '_conn'):
                    if exc_type is None:
                        self._conn.commit()
                    else:
                        self._conn.rollback()
                    self.pool.putconn(self._conn)
            except Exception as e:
                logger.error(f"Error closing connection: {e}")
                raise

    @contextmanager
    def get_cursor(self):
        """Context manager for getting database cursor"""
        conn = self.pool.getconn()
        try:
            cursor = conn.cursor()
            yield cursor
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            self.pool.putconn(conn)

async def save_scraper_data(conn, data: List[Dict], scraper_name: str) -> None:
    """Save scraped data to database in batches"""
    if not data:
        logger.warning(f"No data to save for {scraper_name}")
        return

    cursor = conn.cursor()
    try:
        # Process in batches of 100
        batch_size = 100
        total_saved = 0
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            args = [(
                item['name'],
                item['phone'],
                item['website'],
                item['link'],
                datetime.now(),
                json.dumps(item['raw_data'], ensure_ascii=False)
            ) for item in batch]
            
            cursor.executemany("""
                INSERT INTO leads (name, phone, website, link, scraped_at, raw_data)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (phone) DO UPDATE SET
                    name = EXCLUDED.name,
                    website = EXCLUDED.website,
                    link = EXCLUDED.link,
                    scraped_at = EXCLUDED.scraped_at,
                    raw_data = EXCLUDED.raw_data
            """, args)
            
            total_saved += len(batch)
            logger.info(f"Saved {total_saved} items for {scraper_name}")
            await asyncio.sleep(0)  # Allow other tasks to run
            
    except Exception as e:
        logger.error(f"Error saving batch for {scraper_name}: {e}")
        raise
    finally:
        cursor.close()

async def run_scraper(scraper_module, db_conn) -> None:
    """Run a scraper in a thread pool and save its data"""
    loop = asyncio.get_event_loop()
    scraper_name = scraper_module.__name__.split('.')[-1]
    
    with ThreadPoolExecutor(max_workers=10) as pool:
        try:
            start_time = datetime.now()
            logger.info(f"Starting {scraper_name} scraper")
            
            data = await loop.run_in_executor(pool, scraper_module.scrape)
            
            if data:
                logger.info(f"Found {len(data)} items from {scraper_name}")
                await save_scraper_data(db_conn, data, scraper_name)
                duration = (datetime.now() - start_time).total_seconds()
                logger.info(f"Completed {scraper_name}: {len(data)} items in {duration:.2f}s")
            else:
                logger.warning(f"No data returned from {scraper_name}")
                
        except Exception as e:
            logger.error(f"Error in {scraper_name}: {e}", exc_info=True)

async def main() -> None:
    """Main function to run all scrapers concurrently"""
    scrapers = [
        yeniemlak
    ]
    
    try:
        with DatabaseManager() as db_conn:
            tasks = [run_scraper(scraper, db_conn) for scraper in scrapers]
            start_time = datetime.now()
            await asyncio.gather(*tasks)
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"All scrapers completed in {duration:.2f}s")
            
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        raise

def run() -> None:
    """Entry point with error handling"""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    run()