import aiohttp
from bs4 import BeautifulSoup
import re
from datetime import datetime
import json
import asyncio
import logging
from typing import Dict, List, Optional
from fake_useragent import UserAgent
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class ScraperStats:
    total_pages: int = 0
    total_listings: int = 0
    valid_numbers: int = 0
    invalid_numbers: int = 0
    invalid_phone_list: List[str] = None

    def __post_init__(self):
        self.invalid_phone_list = []

    def print_summary(self):
        print("\nScraping Statistics:")
        print(f"Total pages processed: {self.total_pages}")
        print(f"Total listings found: {self.total_listings}")
        print(f"Valid numbers: {self.valid_numbers}")
        print(f"Invalid numbers: {self.invalid_numbers}")
        if self.invalid_numbers > 0:
            print("\nInvalid phone numbers:")
            for phone in self.invalid_phone_list:
                print(f"  {phone}")

def format_phone(phone: str, stats: Optional[ScraperStats] = None, original: str = None) -> Optional[str]:
    """Format and validate phone number according to Azerbaijan rules"""
    if not phone:
        return None

    # Remove all non-digit characters
    digits = re.sub(r'\D', '', phone)
    
    # Remove country code if present
    if digits.startswith('994'): 
        digits = digits[3:]
    if digits.startswith('0'): 
        digits = digits[1:]
    
    # Validate length
    if len(digits) != 9:
        if stats:
            stats.invalid_phone_list.append(f"Length error - Original: {original}, Cleaned: {digits}")
        return None
    
    # Validate prefix
    valid_prefixes = ('10', '12', '50', '51', '55', '60', '70', '77', '99')
    if not digits.startswith(valid_prefixes):
        if stats:
            stats.invalid_phone_list.append(f"Prefix error - Original: {original}, Cleaned: {digits}")
        return None
    
    # Validate fourth digit
    if digits[2] in ('0', '1'):
        if stats:
            stats.invalid_phone_list.append(f"Fourth digit error - Original: {original}, Cleaned: {digits}")
        return None
        
    return digits

class YeniEmlakScraper:
    def __init__(self):
        self.base_url = "https://yeniemlak.az"
        self.ua = UserAgent()
        self.session = None
        self.stats = ScraperStats()

    def get_headers(self) -> Dict[str, str]:
        """Get randomized headers for requests"""
        return {
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'az,en-US;q=0.7,en;q=0.3',
            'Connection': 'keep-alive'
        }

    async def fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch and parse a page with retries"""
        try:
            await asyncio.sleep(1)  # Rate limiting
            async with self.session.get(url, headers=self.get_headers()) as response:
                if response.status == 200:
                    html = await response.text()
                    return BeautifulSoup(html, 'html.parser')
                logger.warning(f"Got status code {response.status} for {url}")
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
        return None

    async def extract_listing_details(self, url: str) -> Optional[Dict]:
        """Extract details from individual listing page"""
        try:
            soup = await self.fetch_page(url)
            if not soup:
                return None

            details = {}

            # Extract basic information
            price_elem = soup.find('price')
            if price_elem:
                details['price'] = price_elem.text.strip()

            # Extract title 
            title_elem = soup.find('div', class_='title')
            if title_elem:
                details['title'] = title_elem.text.strip()

            # Extract property details
            params = {}
            param_divs = soup.find_all('div', class_='params')
            for div in param_divs:
                text = div.text.strip()
                if 'otaq' in text:
                    params['rooms'] = re.search(r'\d+', text).group()
                elif 'm2' in text:
                    params['area'] = re.search(r'\d+', text).group()
                elif 'sot' in text:
                    params['land_area'] = re.search(r'\d+', text).group()
                elif 'Mərtəbə' in text:
                    floors = re.findall(r'\d+', text)
                    if len(floors) >= 2:
                        params['floor'] = floors[0]
                        params['total_floors'] = floors[1]

            # Extract description
            desc_elem = soup.find('div', class_='text')
            if desc_elem:
                details['description'] = desc_elem.text.strip()

            # Extract seller info and phone
            seller_name = None
            phone = None

            name_elem = soup.find('div', class_='ad')
            if name_elem:
                seller_name = name_elem.text.strip()

            # Phone number is usually in an img element
            phone_img = soup.find('img', src=re.compile(r'/tel-show/'))
            if phone_img:
                phone_number = phone_img['src'].split('/')[-1]
                formatted_phone = format_phone(phone_number, self.stats, phone_number)
                if formatted_phone:
                    phone = formatted_phone
                    self.stats.valid_numbers += 1
                else:
                    self.stats.invalid_numbers += 1
                    return None

            if not phone:
                return None

            return {
                'name': seller_name,
                'phone': phone,
                'website': 'yeniemlak.az',
                'link': url,
                'raw_data': {
                    **details,
                    'params': params
                }
            }

        except Exception as e:
            logger.error(f"Error extracting details from {url}: {e}")
            return None

    async def get_listing_links(self, page: int) -> List[str]:
        """Extract listing links from a search results page"""
        url = f"{self.base_url}/elan/axtar?elan_nov=1&emlak=0&page={page}"
        links = []

        try:
            soup = await self.fetch_page(url)
            if not soup:
                return links

            for table in soup.find_all('table', class_='list'):
                detail_link = table.find('a', class_='detail')
                if detail_link and detail_link.get('href'):
                    full_url = f"{self.base_url}{detail_link['href']}"
                    links.append(full_url)

            return links

        except Exception as e:
            logger.error(f"Error getting listing links from page {page}: {e}")
            return links

    async def scrape_all(self, start_page: int = 1, end_page: int = 3) -> List[Dict]:
        """Main scraping function"""
        async with aiohttp.ClientSession() as session:
            self.session = session
            all_results = []
            
            try:
                for page in range(start_page, end_page + 1):
                    print(f"\nProcessing page {page}")
                    
                    listing_links = await self.get_listing_links(page)
                    print(f"Found {len(listing_links)} listings on page {page}")
                    
                    # Process listings in batches
                    batch_size = 5
                    for i in range(0, len(listing_links), batch_size):
                        batch = listing_links[i:i + batch_size]
                        tasks = [self.extract_listing_details(url) for url in batch]
                        results = await asyncio.gather(*tasks)
                        
                        # Filter out None results
                        valid_results = [r for r in results if r]
                        all_results.extend(valid_results)
                        
                        print(f"Processed batch of {len(valid_results)} listings")
                        await asyncio.sleep(1)  # Rate limiting between batches
                    
                    self.stats.total_pages += 1
                    self.stats.total_listings += len(listing_links)

            except Exception as e:
                logger.error(f"Error during scraping: {e}")
            
            finally:
                self.stats.print_summary()
                
            return all_results

def scrape() -> List[Dict]:
    """Entry point function called by main.py"""
    try:
        return asyncio.run(YeniEmlakScraper().scrape_all())
    except Exception as e:
        logger.error(f"Scraping error: {e}")
        return []

if __name__ == "__main__":
    # For testing the scraper independently
    results = scrape()
    print(f"\nTotal scraped items: {len(results)}")