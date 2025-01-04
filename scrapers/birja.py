import aiohttp
import asyncio
from bs4 import BeautifulSoup, SoupStrainer
from datetime import datetime
import re
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin
import pandas as pd
from pathlib import Path
import logging
import time
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BirjaScraper:
    def __init__(self, test_mode=False, max_pages=None, category_limit=None):
        """
        Initialize scraper with test options:
        test_mode: If True, only scrapes first page of each category
        max_pages: Limit pages per category (None for all pages)
        category_limit: Limit number of categories to scrape (None for all)
        """
        self.base_url = "https://birja.com/all_category/az"
        self.session = None
        self.batch_size = 5000  # Can be adjusted: smaller for testing, larger for production
        self.data = []
        self.processed_urls: Set[str] = set()
        self.sem = asyncio.Semaphore(100)  # Can be adjusted: 100 for production, 10 for testing
        self.connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
        self.data_dir = Path('data')
        self.data_dir.mkdir(exist_ok=True)
        
        # Test mode settings
        self.test_mode = test_mode
        self.max_pages = max_pages if not test_mode else 1
        self.category_limit = category_limit
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        }

    def format_phone(self, phone: str) -> Optional[str]:
        if not phone:
            return None
        digits = ''.join(c for c in phone if c.isdigit())
        if digits.startswith('994'): digits = digits[3:]
        if digits.startswith('0'): digits = digits[1:]
        return digits if len(digits) == 9 and digits[:2] in ('10','12','50','51','55','60','70','77','99') else None

    async def fetch_page(self, url: str) -> Optional[str]:
        async with self.sem:
            try:
                async with self.session.get(url, headers=self.headers) as response:
                    if response.status == 200:
                        return await response.text()
            except Exception as e:
                logger.debug(f"Error fetching {url}: {e}")
            return None

    async def get_categories(self) -> List[Dict]:
        if html := await self.fetch_page(self.base_url):
            soup = BeautifulSoup(html, 'html.parser', parse_only=SoupStrainer(['div', 'h4', 'a']))
            categories = []
            
            for section in soup.find_all('div', class_='col-md-3'):
                if h4 := section.find('h4'):
                    cat_name = h4.text.strip()
                    for link in section.find_all('a', href=True):
                        categories.append({
                            'title': cat_name,
                            'name': link.text.strip(),
                            'url': urljoin(self.base_url, link['href'])
                        })
            
            if self.category_limit:
                categories = categories[:self.category_limit]
                
            logger.info(f"Found {len(categories)} categories" + 
                       (f" (limited to {self.category_limit})" if self.category_limit else ""))
            return categories
        return []

    async def get_listing_urls(self, category_url: str, page: int = 1) -> List[str]:
        if html := await self.fetch_page(f"{category_url}/{page}"):
            parser = BeautifulSoup(html, 'html.parser', parse_only=SoupStrainer('a', class_='cs_card_img'))
            return [
                urljoin(self.base_url, a['href']) 
                for a in parser.find_all('a', href=True)
                if (url := urljoin(self.base_url, a['href'])) not in self.processed_urls 
                and not self.processed_urls.add(url)
            ]
        return []

    async def process_listing(self, url: str) -> Optional[Dict]:
        if html := await self.fetch_page(url):
            try:
                soup = BeautifulSoup(html, 'html.parser', parse_only=SoupStrainer(['h1', 'table', 'div']))
                result = {'url': url, 'scrape_date': datetime.now().isoformat()}
                
                if title := soup.find('h1'):
                    result['title'] = title.text.strip()
                
                if table := soup.find('table', class_='table'):
                    for row in table.find_all('tr'):
                        cols = row.find_all(['td', 'th'])
                        if len(cols) >= 2:
                            key = cols[0].text.strip().replace(':', '')
                            value = cols[1].text.strip()
                            
                            if phone_link := cols[1].find('a', href=lambda x: x and 'tel:' in x):
                                raw_phone = phone_link.text.strip()
                                if formatted := self.format_phone(raw_phone):
                                    result['phone'] = formatted
                                    result['raw_phone'] = raw_phone
                            
                            result[key.lower()] = value
                
                if desc := soup.select_one('div.col-md-6 p'):
                    result['description'] = desc.text.strip()
                
                return result if result.get('phone') else None
            except Exception as e:
                logger.debug(f"Error processing {url}: {e}")
        return None

    async def process_batch(self, batch_data: List[Dict]):
        if batch_data:
            df = pd.DataFrame(batch_data)
            filename = self.data_dir / f'listings_{datetime.now():%Y%m%d_%H%M%S}.csv'
            df.to_csv(filename, index=False, encoding='utf-8')
            logger.info(f"Saved {len(batch_data)} listings to {filename}")

    async def scrape_category(self, category: Dict) -> List[Dict]:
        listings = []
        page = 1
        page_count = 0
        
        while True:
            if self.max_pages and page > self.max_pages:
                break
                
            if not (urls := await self.get_listing_urls(category['url'], page)):
                break
            
            tasks = [self.process_listing(url) for url in urls]
            results = await asyncio.gather(*tasks)
            if valid := [r for r in results if r]:
                listings.extend(valid)
                page_count += 1
                logger.info(f"Category '{category['name']}': Page {page} - Found {len(valid)} listings")
            
            if self.test_mode:
                break
                
            page += 1
        
        logger.info(f"Finished category '{category['name']}' - {len(listings)} total listings from {page_count} pages")
        return listings

    async def scrape(self):
        async with aiohttp.ClientSession(connector=self.connector) as session:
            self.session = session
            start = time.time()
            
            if not (categories := await self.get_categories()):
                return
            
            chunk_size = 20  # Production: 20, Testing: 5
            total = 0
            
            for i in range(0, len(categories), chunk_size):
                chunk = categories[i:i + chunk_size]
                tasks = [self.scrape_category(cat) for cat in chunk]
                
                for listings in await asyncio.gather(*tasks):
                    if listings:
                        total += len(listings)
                        self.data.extend(listings)
                        
                        while len(self.data) >= self.batch_size:
                            batch, self.data = self.data[:self.batch_size], self.data[self.batch_size:]
                            await self.process_batch(batch)
                            elapsed = time.time() - start
                            logger.info(f"Progress: {total:,} listings ({total/elapsed:.1f}/sec)")
            
            if self.data:
                await self.process_batch(self.data)
            
            logger.info(f"Scraping completed: {total:,} listings in {time.time() - start:.1f}s")

def parse_args():
    parser = argparse.ArgumentParser(description='Birja.com Scraper')
    parser.add_argument('--test', action='store_true', help='Run in test mode (1 page per category)')
    parser.add_argument('--max-pages', type=int, help='Maximum pages per category')
    parser.add_argument('--category-limit', type=int, help='Limit number of categories')
    return parser.parse_args()

async def main():
    args = parse_args()
    
    # Uncomment configuration options as needed:
    scraper = BirjaScraper(
        test_mode=args.test,  # True for testing, False for production
        max_pages=args.max_pages,  # None for all pages, or set a number
        category_limit=args.category_limit  # None for all categories, or set a number
    )
    
    await scraper.scrape()

if __name__ == "__main__":
    # Usage examples:
    # Full scrape:     python3 scrapers/birja.py
    # Test mode:       python3 scrapers/birja.py --test
    # Limited pages:   python3 scrapers/birja.py --max-pages 5
    # Limited cats:    python3 scrapers/birja.py --category-limit 3
    # Combined:        python scraper.py --test --category-limit 2
    asyncio.run(main())