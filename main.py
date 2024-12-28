import asyncio
from scraper import AutonetScraper
import logging

logger = logging.getLogger(__name__)

async def main():
    scraper = AutonetScraper()

    try:
        # Scrape all pages (adjust range as needed)
        results = await scraper.scrape(start_page=1, end_page=236)
        
        if results:
            # Optionally save to CSV as backup
            scraper.save_to_csv()
            logger.info(f"Successfully scraped {len(results)} listings")
        else:
            logger.error("No results were scraped")
    except Exception as e:
        logger.error(f"Scraping failed: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())