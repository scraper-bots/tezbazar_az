import asyncio
from scraper import AutonetScraper
import logging

logger = logging.getLogger(__name__)

async def main():
    scraper = AutonetScraper()

    try:
        await scraper.scrape(start_page=1, end_page=236)
    except Exception as e:
        logger.error(f"Scraping failed: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())