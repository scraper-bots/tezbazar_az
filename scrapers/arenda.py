import asyncio
import aiohttp
import csv
import time
import os
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin
import logging
from datetime import datetime
import random
from pathlib import Path
from tqdm import tqdm

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class ArendaScraper:
    def __init__(self):
        self.base_url = "https://arenda.az"
        self.listings = []
        self.session = None
        self.total_pages = 628  # Total number of pages to scrape
        self.semaphore = asyncio.Semaphore(15)  # Increased concurrent requests
        self.processed_urls = set()
        
        # Create data directory if it doesn't exist
        self.data_dir = Path('data')
        self.data_dir.mkdir(exist_ok=True)

    async def init_session(self):
        """Initialize aiohttp session with headers"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        }
        self.session = aiohttp.ClientSession(headers=headers)

    async def fetch_page(self, url):
        """Fetch page content with retry logic and semaphore"""
        async with self.semaphore:
            for _ in range(3):
                try:
                    async with self.session.get(url) as response:
                        if response.status == 200:
                            content = await response.read()
                            return content.decode('utf-8', errors='ignore')
                        await asyncio.sleep(0.3)  # Reduced sleep time
                except Exception as e:
                    logging.error(f"Error fetching {url}: {str(e)}")
                    await asyncio.sleep(0.5)
            return None

    async def scrape_listing_details(self, url, pbar):
        """Scrape individual listing details"""
        if url in self.processed_urls:
            return None
            
        try:
            html = await self.fetch_page(url)
            if not html:
                return None

            self.processed_urls.add(url)
            soup = BeautifulSoup(html, 'html.parser')
            
            details = {
                'url': url,
                'title': '',
                'price': '',
                'phone': '',
                'address': '',
                'description': '',
                'features': '',
                'property_info': '',
                'date_posted': '',
                'listing_id': '',
                'scrape_timestamp': datetime.now().isoformat()
            }

            # Extract all details
            if title_elem := soup.select_one('.elan_main_title'):
                details['title'] = title_elem.text.strip()

            if price_elem := soup.select_one('.elan_new_price_box'):
                details['price'] = price_elem.text.strip()

            if phone_elem := soup.select_one('.elan_in_tel'):
                details['phone'] = phone_elem.text.strip()

            if address_elem := soup.select_one('.elan_unvan_txt'):
                details['address'] = address_elem.text.strip()

            if desc_elem := soup.select_one('.elan_info_txt'):
                details['description'] = desc_elem.text.strip()

            if features := soup.select('.property_lists li'):
                details['features'] = ', '.join([f.text.strip() for f in features])

            if property_info := soup.select('.n_elan_box_botom_params td'):
                details['property_info'] = ', '.join([p.text.strip() for p in property_info])

            if date_elem := soup.select_one('.elan_date_box_rside p'):
                details['date_posted'] = date_elem.text.strip()

            if listing_id := soup.select_one('.elan_date_box_rside p:nth-child(2)'):
                details['listing_id'] = listing_id.text.strip()

            pbar.update(1)
            return details

        except Exception as e:
            logging.error(f"Error scraping listing {url}: {str(e)}")
            return None

    async def scrape_listing_urls(self, page_num):
        """Scrape listing URLs from a page"""
        url = f"https://arenda.az/filtirli-axtaris/{page_num}/?home_search=1&lang=1&site=1&home_s=1"
        html = await self.fetch_page(url)
        if not html:
            return []

        soup = BeautifulSoup(html, 'html.parser')
        listing_elements = soup.select('.new_elan_box a')
        urls = [urljoin(self.base_url, elem.get('href')) for elem in listing_elements if elem.get('href')]
        logging.info(f"Found {len(urls)} listings on page {page_num}")
        return urls

    async def save_to_csv(self, filename=None):
        """Save listings to CSV in data directory"""
        if not self.listings:
            return

        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'arenda_listings_{timestamp}.csv'
        
        filepath = self.data_dir / filename
        df = pd.DataFrame(self.listings)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        logging.info(f"Saved {len(self.listings)} listings to {filepath}")

    async def scrape_page(self, page_num, pbar):
        """Scrape all listings from a single page"""
        try:
            listing_urls = await self.scrape_listing_urls(page_num)
            # Create tasks for all listings on the page
            tasks = [self.scrape_listing_details(url, pbar) for url in listing_urls]
            # Execute tasks concurrently
            results = await asyncio.gather(*tasks)
            
            # Filter out None results and add to listings
            valid_results = [r for r in results if r]
            self.listings.extend(valid_results)
            
            # Save progress every 10 pages
            if page_num % 10 == 0:
                await self.save_to_csv(f'arenda_listings_progress_{page_num}.csv')
            
        except Exception as e:
            logging.error(f"Error scraping page {page_num}: {str(e)}")

    async def run(self):
        """Main scraping function"""
        try:
            start_time = time.time()
            await self.init_session()
            
            # Create progress bar
            total_listings = self.total_pages * 68  # Approximate number of listings
            with tqdm(total=total_listings, desc="Scraping listings") as pbar:
                # Create tasks for all pages
                tasks = [self.scrape_page(page, pbar) for page in range(1, self.total_pages + 1)]
                await asyncio.gather(*tasks)
            
            # Save final results
            await self.save_to_csv('arenda_listings_final.csv')
            
            end_time = time.time()
            duration = end_time - start_time
            items_per_second = len(self.listings) / duration
            logging.info(f"Scraping completed in {duration:.2f} seconds")
            logging.info(f"Average speed: {items_per_second:.2f} items/second")
            logging.info(f"Total unique listings scraped: {len(self.listings)}")
            
        except Exception as e:
            logging.error(f"Error in main scraping process: {str(e)}")
        finally:
            if self.session:
                await self.session.close()

async def main():
    scraper = ArendaScraper()
    await scraper.run()

if __name__ == "__main__":
    asyncio.run(main())