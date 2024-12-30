import logging
import time
from typing import Dict, List, Optional, Set
from bs4 import BeautifulSoup
import asyncio
import aiohttp
from .base import BaseScraper

logger = logging.getLogger(__name__)

class ArendaScraper(BaseScraper):
    def __init__(self, base_url: str, db_manager):
        super().__init__(base_url, db_manager)
        self.base_url = "https://arenda.az"
        self.search_url = "https://arenda.az/filtirli-axtaris/"
        self.session.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.leads_batch = []
        self.batch_size = 25  # Increased batch size
        self.processed_ids: Set[str] = set()  # Track processed listings

    async def get_page_count(self, session) -> int:
        """Get total number of pages"""
        try:
            async with session.get(self.search_url) as response:
                html = await self.decode_response(await response.read())
                soup = BeautifulSoup(html, 'html.parser')
                
                # Look for pagination box
                pagination = soup.select_one('div.pagination_box ul.pagination')
                if pagination:
                    # Find all page number links
                    page_links = pagination.select('a.page-numbers')
                    page_numbers = []
                    
                    for link in page_links:
                        try:
                            num = int(link.text.strip())
                            page_numbers.append(num)
                        except ValueError:
                            continue
                    
                    if page_numbers:
                        return max(page_numbers)
                
                # Default to 3 pages if pagination not found
                logger.warning("Pagination not found, defaulting to 3 pages")
                return 3
                
        except Exception as e:
            logger.error(f"Error getting page count: {str(e)}")
            return 3
    
    async def decode_response(self, raw_bytes: bytes) -> str:
        """Decode response with proper encoding handling"""
        encodings = ['utf-8', 'windows-1251', 'utf-8-sig', 'ascii']
        for encoding in encodings:
            try:
                return raw_bytes.decode(encoding)
            except UnicodeDecodeError:
                continue
        return raw_bytes.decode('utf-8', errors='ignore')

    async def fetch_listings(self, session: aiohttp.ClientSession, page: int) -> List[Dict]:
        """Fetch listings from a single page"""
        params = {
            'home_search': '1',
            'lang': '1',
            'site': '1',
            'home_s': '1',
            'page': str(page)
        }
        
        try:
            async with session.get(self.search_url, params=params) as response:
                html = await self.decode_response(await response.read())
                listings = self.parse_listing_page(html)
                logger.info(f"Found {len(listings)} listings on page {page}")
                return listings
        except Exception as e:
            logger.error(f"Error fetching page {page}: {str(e)}")
            return []

    def parse_listing_page(self, html: str) -> List[Dict]:
        soup = BeautifulSoup(html, 'html.parser')
        listings = []
        
        for item in soup.select('li.new_elan_box'):
            try:
                link_elem = item.select_one('a')
                if not link_elem:
                    continue

                listing_id = item.get('id', '').replace('elan_', '')
                if listing_id in self.processed_ids:
                    continue

                self.processed_ids.add(listing_id)
                listing = {
                    'id': listing_id,
                    'link': self.base_url + link_elem.get('href', ''),
                    'title': ' '.join([elem.text.strip() for elem in item.select('.elan_property_title')]),
                    'price': self.extract_price(item.select_one('.elan_price').text.strip() if item.select_one('.elan_price') else ''),
                    'location': item.select_one('.elan_unvan').text.strip() if item.select_one('.elan_unvan') else '',
                    'date': item.select_one('.elan_box_date').text.strip() if item.select_one('.elan_box_date') else '',
                    'details': self.parse_property_details(item)
                }
                listings.append(listing)
            except Exception as e:
                logger.error(f"Error parsing listing: {str(e)}")
                
        return listings

    def parse_property_details(self, item: BeautifulSoup) -> Dict:
        """Extract property details from listing item"""
        details = {}
        try:
            params_table = item.select_one('.n_elan_box_botom_params')
            if params_table:
                for cell in params_table.select('td'):
                    text = cell.text.strip()
                    if 'otaqlı' in text:
                        details['rooms'] = text.split()[0]
                    elif 'm²' in text:
                        details['area'] = text.replace('m²', '').strip()
                    elif 'mərtəbə' in text:
                        details['floor'] = text
        except Exception as e:
            logger.error(f"Error parsing property details: {str(e)}")
        return details

    async def fetch_details(self, session: aiohttp.ClientSession, url: str) -> Optional[Dict]:
        """Fetch details for a single listing"""
        try:
            # Ensure URL is properly formatted
            if url.startswith('https://arenda.azhttps://'):
                url = url.replace('https://arenda.azhttps://', 'https://')
            elif not url.startswith('http'):
                url = 'https://' + url.lstrip('/')
            
            async with session.get(url) as response:
                if response.status != 200:
                    logger.error(f"Error fetching {url}: Status {response.status}")
                    return None
                html = await self.decode_response(await response.read())
                return self.parse_listing_details(html)
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching details from {url}")
            return None
        except Exception as e:
            logger.error(f"Error fetching details from {url}: {str(e)}")
            return None

    def parse_listing_details(self, html: str) -> Dict:
        soup = BeautifulSoup(html, 'html.parser')
        details = {}
        
        try:
            user_info = soup.select_one('.new_elan_user_info')
            if user_info:
                details['name'] = user_info.select_one('p').text.strip() if user_info.select_one('p') else ''
                details['phones'] = [
                    self.clean_phone_number(phone.text.strip())
                    for phone in user_info.select('.elan_in_tel')
                    if phone.text.strip()
                ]

            details['features'] = [
                feature.text.strip()
                for feature in soup.select('.property_lists li')
            ]

            if desc_elem := soup.select_one('.elan_info_txt'):
                details['description'] = desc_elem.text.strip()

            if addr_elem := soup.select_one('.elan_unvan_txt'):
                details['address'] = addr_elem.text.strip()

        except Exception as e:
            logger.error(f"Error parsing details: {str(e)}")

        return details

    def clean_phone_number(self, phone: str) -> str:
        phone = ''.join(filter(str.isdigit, phone))
        if len(phone) >= 9:
            if phone.startswith('994'):
                return '+' + phone
            elif phone.startswith('0'):
                return '+994' + phone[1:]
            return '+994' + phone
        return ''

    def extract_price(self, price_text: str) -> Dict:
        """Extract and normalize price information"""
        default_return = {'amount': 0, 'currency': 'AZN'}
        try:
            # Remove all non-digit characters except decimal point
            amount_str = ''.join(filter(lambda x: x.isdigit() or x == '.', price_text))
            if not amount_str:
                return default_return
            
            try:
                amount = float(amount_str)
            except ValueError:
                return default_return

            return {
                'amount': amount,
                'currency': 'AZN' if 'AZN' in price_text else ''
            }
        except Exception as e:
            logger.error(f"Error extracting price from {price_text}: {str(e)}")
            return default_return

    async def process_batch(self, session: aiohttp.ClientSession, listings: List[Dict]) -> None:
        """Process a batch of listings in parallel"""
        tasks = [self.fetch_details(session, listing['link']) for listing in listings]
        details_list = await asyncio.gather(*tasks, return_exceptions=True)
        
        leads_batch = []
        for listing, details in zip(listings, details_list):
            try:
                if isinstance(details, dict) and details:
                    listing.update(details)
                    leads = self.extract_lead_data(listing)
                    if leads:
                        leads_batch.extend(leads)
            except Exception as e:
                logger.error(f"Error processing listing {listing.get('id')}: {str(e)}")
        
        if leads_batch:
            try:
                self.db_manager.save_leads_batch(leads_batch)
                await asyncio.sleep(0.5)  # Short delay between batches
            except Exception as e:
                logger.error(f"Error saving leads batch: {str(e)}")

    def extract_lead_data(self, listing_data: Dict) -> List[Dict]:
        leads = []
        for phone in listing_data.get('phones', []):
            if not phone:
                continue
            leads.append({
                'name': listing_data.get('name', ''),
                'phone': phone,
                'website': 'arenda.az',
                'link': listing_data.get('link', ''),
                'raw_data': {
                    'title': listing_data.get('title', ''),
                    'price': listing_data.get('price', {}),
                    'location': listing_data.get('location', ''),
                    'address': listing_data.get('address', ''),
                    'details': listing_data.get('details', {}),
                    'features': listing_data.get('features', []),
                    'description': listing_data.get('description', ''),
                    'date': listing_data.get('date', ''),
                    'id': listing_data.get('id', '')
                }
            })
        return leads

    async def run_async(self):
        conn = aiohttp.TCPConnector(limit=20, force_close=True)
        timeout = aiohttp.ClientTimeout(total=60, connect=30, sock_read=30)
        
        async with aiohttp.ClientSession(
            connector=conn,
            timeout=timeout,
            headers=self.session.headers,
            raise_for_status=True
        ) as session:
            total_pages = await self.get_page_count(session)
            
            # Fetch all pages in parallel
            tasks = [self.fetch_listings(session, page) for page in range(1, total_pages + 1)]
            all_pages = await asyncio.gather(*tasks)
            
            # Flatten listings
            all_listings = [item for sublist in all_pages for item in sublist]
            logger.info(f"Found {len(all_listings)} total listings")
            
            # Process in larger batches
            for i in range(0, len(all_listings), self.batch_size):
                batch = all_listings[i:i + self.batch_size]
                try:
                    await self.process_batch(session, batch)
                except Exception as e:
                    logger.error(f"Error processing batch {i//self.batch_size + 1}: {str(e)}")
                    continue
                
            return all_listings

    def run(self):
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                listings = loop.run_until_complete(self.run_async())
            finally:
                loop.close()
            
            self.db_manager.flush_batch()
            
        except Exception as e:
            logger.error(f"Error in scraping process: {str(e)}")
            raise