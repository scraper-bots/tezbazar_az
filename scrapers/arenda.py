import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime
import json
import re
from fake_useragent import UserAgent
import random
import asyncio
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class ArendaScraper:
    def __init__(self):
        self.base_url = "https://arenda.az/filtirli-axtaris/{page}/?home_search=1&lang=1&site=1"
        self.ua = UserAgent()
        self.session = None

    def get_headers(self) -> Dict[str, str]:
        return {
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive'
        }

    async def get_listing_details(self, url: str) -> Optional[Dict]:
        try:
            await asyncio.sleep(random.uniform(0.5, 1))
            async with self.session.get(url, headers=self.get_headers()) as response:
                if response.status != 200:
                    return None
                    
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')

                name = None
                phone = None
                user_info = soup.find('div', class_='new_elan_user_info')
                if user_info:
                    name_elem = user_info.find('p')
                    if name_elem:
                        name = name_elem.text.strip().split('(')[0].strip()
                    phone_elem = user_info.find('a', class_='elan_in_tel')
                    if phone_elem:
                        phone = re.sub(r'\D', '', phone_elem.text)

                raw_data = {
                    'title': soup.find('h2', class_='elan_main_title').text.strip() if soup.find('h2', class_='elan_main_title') else None,
                    'description': soup.find('div', class_='elan_info_txt').text.strip() if soup.find('div', class_='elan_info_txt') else None,
                    'address': soup.find('span', class_='elan_unvan_txt').text.strip() if soup.find('span', class_='elan_unvan_txt') else None,
                    'price': soup.find('div', class_='elan_new_price_box').text.strip() if soup.find('div', class_='elan_new_price_box') else None,
                    'details': [item.text.strip() for item in soup.find_all('li', class_='property_lists')] if soup.find('ul', class_='property_lists') else [],
                    'scraped_datetime': datetime.now().isoformat()
                }

                return {
                    'name': name,
                    'phone': phone,
                    'website': 'arenda.az',
                    'link': url,
                    'raw_data': raw_data
                }

        except Exception as e:
            logger.error(f"Error getting listing details from {url}: {e}")
            return None

    async def process_page(self, page: int) -> List[Dict]:
        url = self.base_url.format(page=page)
        try:
            async with self.session.get(url, headers=self.get_headers()) as response:
                if response.status != 200:
                    logger.warning(f"Failed to fetch page {page}: Status {response.status}")
                    return []
                    
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                listings = soup.find_all('li', class_='new_elan_box')
                
                tasks = []
                for listing in listings:
                    link = listing.find('a')
                    if link and 'href' in link.attrs:
                        full_url = link['href']
                        if not full_url.startswith('http'):
                            full_url = f"https://arenda.az{full_url}"
                        tasks.append(self.get_listing_details(full_url))

                results = await asyncio.gather(*tasks)
                return [r for r in results if r is not None]

        except Exception as e:
            logger.error(f"Error processing page {page}: {e}")
            return []

    async def scrape_all(self, start_page: int = 1, end_page: int = 3) -> List[Dict]:
        async with aiohttp.ClientSession() as session:
            self.session = session
            all_results = []
            
            for batch_start in range(start_page, end_page + 1, 5):
                batch_end = min(batch_start + 4, end_page)
                pages = range(batch_start, batch_end + 1)
                
                try:
                    # Process pages in batches
                    results = await asyncio.gather(*[self.process_page(page) for page in pages])
                    
                    # Flatten results and add to main list
                    for page_results in results:
                        all_results.extend(page_results)
                    
                    logger.info(f"Completed pages {batch_start}-{batch_end}, total items: {len(all_results)}")
                    await asyncio.sleep(1)  # Delay between batches
                    
                except Exception as e:
                    logger.error(f"Error processing batch {batch_start}-{batch_end}: {e}")
                    continue
            
            return all_results

def scrape() -> List[Dict]:
    """Main scraping function called by main.py"""
    try:
        return asyncio.run(ArendaScraper().scrape_all())
    except Exception as e:
        logger.error(f"Scraping error: {e}")
        return []

if __name__ == "__main__":
    # For testing the scraper independently
    results = scrape()
    print(f"Total scraped items: {len(results)}")