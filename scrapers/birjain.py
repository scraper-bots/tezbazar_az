import logging
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from typing import Dict, List, Set
from .base import BaseScraper
import concurrent.futures
from itertools import chain
import time

logger = logging.getLogger(__name__)

class BirjaInScraper(BaseScraper):
    def __init__(self, base_url: str, db_manager):
        super().__init__(base_url, db_manager)
        self.base_url = "https://birja-in.az"
        self.search_url = f"{self.base_url}/elanlar"
        self.session.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'az-AZ,az;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

        # Connection pooling and timeout settings
        self.timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_connect=10, sock_read=10)
        self.conn_limit = 100
        
        # Rate limiting
        self.request_semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        
        # Connection pools
        self.session_pool = []

    def get_page_url(self, page: int) -> str:
        """Get URL for specific page"""
        if page == 1:
            return f"{self.search_url}/"
        return f"{self.search_url}/page{page}.html"

    async def create_session_pool(self):
        """Create a pool of aiohttp sessions for concurrent requests"""
        connector = aiohttp.TCPConnector(limit=self.conn_limit, force_close=True)
        for _ in range(self.max_workers):
            session = aiohttp.ClientSession(
                connector=connector,
                headers=self.session.headers,
                timeout=self.timeout
            )
            self.session_pool.append(session)
        return self.session_pool

    async def get_session(self):
        """Get a session from the pool using round-robin"""
        if not self.session_pool:
            await self.create_session_pool()
        session_idx = int(time.time() * 1000) % len(self.session_pool)
        return self.session_pool[session_idx]

    async def fetch_with_retry(self, url: str, max_retries: int = 3) -> str:
        """Fetch URL with retry logic and rate limiting"""
        async with self.request_semaphore:
            for attempt in range(max_retries):
                try:
                    session = await self.get_session()
                    async with session.get(url) as response:
                        if response.status == 200:
                            return await response.text()
                        elif response.status == 429:  # Too Many Requests
                            wait_time = min(2 ** attempt, 8)
                            await asyncio.sleep(wait_time)
                        else:
                            logger.error(f"Error {response.status} fetching {url}")
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(f"Failed to fetch {url} after {max_retries} attempts: {e}")
                    await asyncio.sleep(1)
            return ""

    async def get_page_count(self, session: aiohttp.ClientSession) -> int:
        """Get total number of pages"""
        try:
            html = await self.fetch_with_retry(self.search_url)
            if not html:
                return 1

            soup = BeautifulSoup(html, 'html.parser')
            nav_text = soup.select_one('div.navigator_page_all_advert')
            if nav_text:
                text = nav_text.get_text()
                try:
                    pages = int(text.split('Səhifələr:')[1].split()[0])
                    logger.info(f"Found total pages in nav text: {pages}")
                    return pages
                except (IndexError, ValueError) as e:
                    logger.error(f"Error parsing nav text: {str(e)}")

            return 1
        except Exception as e:
            logger.error(f"Error getting page count: {str(e)}")
            return 1

    async def fetch_pages_concurrent(self, total_pages: int) -> List[str]:
        """Fetch multiple pages concurrently in batches"""
        all_html = []
        
        for batch_start in range(1, total_pages + 1, self.page_batch_size):
            batch_end = min(batch_start + self.page_batch_size, total_pages + 1)
            batch_urls = [
                self.get_page_url(page)
                for page in range(batch_start, batch_end)
            ]
            
            tasks = [self.fetch_with_retry(url) for url in batch_urls]
            batch_results = await asyncio.gather(*tasks)
            all_html.extend(batch_results)
            
            await asyncio.sleep(0.1)
            
        return all_html

    def parse_listing_page(self, html: str) -> List[Dict]:
        """Parse the listings from a page"""
        if not html:
            return []

        soup = BeautifulSoup(html, 'html.parser')
        listings = []

        listing_blocks = soup.select('div.block_one_synopsis_advert_picked, div.block_one_synopsis_advert_fon')
        logger.info(f"Found {len(listing_blocks)} listing blocks")

        for block in listing_blocks:
            try:
                listing_div = block.select_one('div.block_content_synopsis_adv')
                if not listing_div:
                    continue

                title_elem = listing_div.select_one('h2 a')
                if not title_elem:
                    continue

                listing_id = title_elem.get('href', '').split('adv')[-1].split('.')[0]
                if not listing_id or listing_id in self.processed_ids:
                    continue

                self.processed_ids.add(listing_id)

                listing = {
                    'id': listing_id,
                    'title': title_elem.get_text(strip=True),
                    'link': self.base_url + title_elem.get('href', ''),
                    'price': self.extract_price(listing_div),
                    'location': self.extract_location(listing_div),
                    'category': self.extract_category(listing_div)
                }
                
                logger.debug(f"Extracted listing {listing_id}: {listing['title']}")
                listings.append(listing)

            except Exception as e:
                logger.error(f"Error parsing listing block: {str(e)}")

        logger.info(f"Successfully parsed {len(listings)} listings")
        return listings

    def extract_price(self, item) -> Dict:
        """Extract price information"""
        try:
            price_div = item.select_one('div.block_cost_advert_synopsis_search')
            if price_div:
                price_span = price_div.select_one('span.value_cost_adv')
                currency_span = price_div.select_one('span.value_currency')
                
                if price_span:
                    try:
                        amount = float(price_span.text.strip().replace(' ', ''))
                    except ValueError:
                        amount = 0
                else:
                    amount = 0
                    
                currency = currency_span.text.strip() if currency_span else 'AZN'
                return {'amount': amount, 'currency': currency}
        except Exception as e:
            logger.error(f"Error extracting price: {str(e)}")
        return {'amount': 0, 'currency': 'AZN'}

    def extract_location(self, item) -> str:
        """Extract location information"""
        try:
            location_div = item.select_one('div.block_name_region_adv')
            if location_div:
                text = location_div.get_text(strip=True)
                text = text.replace('<!--Bakı-->', '').strip()
                return text
        except Exception as e:
            logger.error(f"Error extracting location: {str(e)}")
        return ''

    def extract_category(self, item) -> str:
        """Extract category"""
        try:
            category_span = item.select_one('div.block_name_category_adv span[style*="color: #ea6f24"]')
            if category_span:
                return category_span.text.strip()
        except Exception as e:
            logger.error(f"Error extracting category: {str(e)}")
        return ''

    def parse_html_parallel(self, html_pages: List[str]) -> List[Dict]:
        """Parse multiple HTML pages in parallel using ThreadPoolExecutor"""
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            parsed_pages = list(executor.map(self.parse_listing_page, html_pages))
        return list(chain.from_iterable(parsed_pages))

    async def fetch_listing_details(self, url: str) -> Dict:
        """Fetch and parse listing details"""
        html = await self.fetch_with_retry(url)
        if not html:
            return {}
            
        try:
            soup = BeautifulSoup(html, 'html.parser')
            details = {
                'description': '',
                'contact': {'name': '', 'phones': []},
                'parameters': {}
            }

            if desc_td := soup.select_one('td.td_text_advert'):
                details['description'] = desc_td.text.strip()

            if contact_table := soup.find('table', class_='contact'):
                if name_cell := contact_table.find('td', class_='name_adder'):
                    details['contact']['name'] = name_cell.text.strip()

                for row in contact_table.find_all('tr'):
                    if phone_label := row.find('td', class_='td_name_param_phone'):
                        if phone_cell := phone_label.find_next_sibling('td'):
                            phone_text = phone_cell.text.strip()
                            phones = [
                                self.clean_phone_number(part.strip())
                                for part in phone_text.replace(',', ' ').replace(';', ' ').split()
                            ]
                            details['contact']['phones'].extend(
                                phone for phone in phones 
                                if phone and len(phone) >= 9
                            )

            return details
        except Exception as e:
            logger.error(f"Error parsing listing details from {url}: {e}")
            return {}

    def clean_phone_number(self, phone: str) -> str:
        """Clean and format phone number"""
        phone = ''.join(filter(str.isdigit, phone))
        if len(phone) >= 9:
            if phone.startswith('994'):
                return '+' + phone
            elif phone.startswith('0'):
                return '+994' + phone[1:]
            return '+994' + phone
        return phone

    def extract_lead_data(self, listing_data: Dict, details: Dict) -> List[Dict]:
        """Convert listing and details into lead format"""
        leads = []
        
        phones = details.get('contact', {}).get('phones', [])
        if not phones:
            logger.debug(f"No phone numbers found for listing {listing_data.get('id')}")
            return leads

        for phone in phones:
            lead_data = {
                'name': details.get('contact', {}).get('name', ''),
                'phone': phone,
                'website': 'birja-in.az',
                'link': listing_data.get('link', ''),
                'raw_data': {
                    'title': listing_data.get('title', ''),
                    'price': listing_data.get('price', {}),
                    'location': listing_data.get('location', ''),
                    'category': listing_data.get('category', ''),
                    'description': details.get('description', ''),
                    'id': listing_data.get('id', '')
                }
            }
            leads.append(lead_data)
            logger.debug(f"Created lead data for phone {phone}: {lead_data['name']}")
        
        return leads

    async def process_listings_parallel(self, listings: List[Dict]) -> None:
        """Process listings in parallel batches"""
        for i in range(0, len(listings), self.listing_batch_size):
            batch = listings[i:i + self.listing_batch_size]
            tasks = [self.fetch_listing_details(listing['link']) for listing in batch]
            details_list = await asyncio.gather(*tasks)
            
            leads_batch = []
            for listing, details in zip(batch, details_list):
                if details:
                    leads = self.extract_lead_data(listing, details)
                    leads_batch.extend(leads)
            
            if leads_batch:
                self.db_manager.save_leads_batch(leads_batch)
                await asyncio.sleep(0.1)

    async def run_async(self):
        """Main scraping process with parallel execution"""
        try:
            # Get total pages
            session = await self.get_session()
            total_pages = await self.get_page_count(session)
            logger.info(f"Starting scrape of {total_pages} pages")
            
            # Fetch all pages concurrently
            html_pages = await self.fetch_pages_concurrent(total_pages)
            
            # Parse listings in parallel
            all_listings = self.parse_html_parallel(html_pages)
            logger.info(f"Found {len(all_listings)} total listings")
            
            # Process listings in parallel
            await self.process_listings_parallel(all_listings)
            
            return all_listings
            
        except Exception as e:
            logger.error(f"Error in main scraping process: {e}")
            return []
        finally:
            # Clean up session pool
            if self.session_pool:
                await asyncio.gather(*[session.close() for session in self.session_pool])

    def run(self):
        """Entry point with proper resource cleanup"""
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                listings = loop.run_until_complete(self.run_async())
                self.db_manager.flush_batch()
                return listings
            finally:
                loop.close()
        except Exception as e:
            logger.error(f"Error in scraping process: {e}")
            raise