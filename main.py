# main.py
import asyncio
import os
from dotenv import load_dotenv
import psycopg2
from concurrent.futures import ThreadPoolExecutor
from scrapers import arenda
# Import other scrapers as they are created

# Load environment variables
load_dotenv()

def get_db_connection():
    """Create and return database connection using environment variables"""
    return psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )

async def run_scraper(scraper_module, db_connection):
    """Run a scraper in a thread pool and save results to database"""
    loop = asyncio.get_event_loop()
    
    # Run the scraper in a thread pool since most scrapers are synchronous
    with ThreadPoolExecutor() as pool:
        try:
            # Run the scraper
            data = await loop.run_in_executor(pool, scraper_module.scrape)
            print(f"Completed scraping with {scraper_module.__name__}, found {len(data)} items")
            
            # Save to database using the module's save_to_db function
            if data:  # Only try to save if we have data
                await loop.run_in_executor(pool, scraper_module.save_to_db, db_connection, data)
                print(f"Saved data from {scraper_module.__name__} to database")
            
        except Exception as e:
            print(f"Error in {scraper_module.__name__}: {e}")

async def main():
    """Main function to run all scrapers concurrently"""
    # List of all scraper modules
    scrapers = [
        arenda,
        # Add other scrapers here
    ]
    
    try:
        # Create database connection
        conn = get_db_connection()
        print("Successfully connected to database")
        
        # Create tasks for all scrapers
        tasks = [run_scraper(scraper, conn) for scraper in scrapers]
        
        # Run all scrapers concurrently
        await asyncio.gather(*tasks)
        
    except Exception as e:
        print(f"Error in main: {e}")
    
    finally:
        # Close database connection
        if 'conn' in locals():
            conn.close()
            print("Database connection closed")

if __name__ == "__main__":
    asyncio.run(main())