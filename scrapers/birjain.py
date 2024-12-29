import logging
import time
from typing import Dict, List
from bs4 import BeautifulSoup
import asyncio
import aiohttp
from .base import BaseScraper

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
        self.processed_ids = set()

    def get_page_url(self, page: int) -> str:
        """Get URL for specific page"""
        if page == 1:
            return f"{self.search_url}/"
        return f"{self.search_url}/page{page}.html"

    async def get_page_count(self, session: aiohttp.ClientSession) -> int:
        """Get total number of pages"""
        try:
            async with session.get(self.search_url) as response:
                if response.status != 200:
                    logger.error(f"Failed to fetch page count: {response.status}")
                    return 1

                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Look for total pages in navigation
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

    async def fetch_page(self, session: aiohttp.ClientSession, page: int) -> List[Dict]:
        """Fetch listings from a single page"""
        url = self.get_page_url(page)
        logger.info(f"Fetching page {page} from URL: {url}")
        
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    logger.error(f"Failed to fetch page {page}: {response.status}")
                    return []
                    
                html = await response.text()
                return self.parse_listing_page(html)
        except Exception as e:
            logger.error(f"Error fetching page {page}: {str(e)}")
            return []

    def parse_listing_page(self, html: str) -> List[Dict]:
        """Parse the listings from a page"""
        soup = BeautifulSoup(html, 'html.parser')
        listings = []

        # Find all listing blocks
        listing_blocks = soup.select('div.block_one_synopsis_advert_picked, div.block_one_synopsis_advert_fon')
        logger.info(f"Found {len(listing_blocks)} listing blocks")

        for block in listing_blocks:
            try:
                listing_div = block.select_one('div.block_content_synopsis_adv')
                if not listing_div:
                    continue

                # Extract listing data
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

    async def fetch_listing_details(self, session: aiohttp.ClientSession, url: str) -> Dict:
        """Fetch detailed listing information"""
        try:
            logger.info(f"Fetching details from {url}")
            async with session.get(url) as response:
                if response.status != 200:
                    logger.error(f"Failed to fetch listing details: {response.status}")
                    return {}
                    
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                details = {
                    'description': '',
                    'contact': {},
                    'parameters': {}
                }

                # Extract description
                desc_td = soup.select_one('td.td_text_advert')
                if desc_td:
                    details['description'] = desc_td.text.strip()

                # Extract contact info from contact table
                contact_table = soup.find('table', class_='contact')
                if contact_table:
                    # Extract name
                    name_cell = contact_table.find('td', class_='name_adder')
                    if name_cell:
                        details['contact']['name'] = name_cell.text.strip()
                    
                    # Extract phone
                    phone_rows = contact_table.find_all('tr')
                    for row in phone_rows:
                        phone_label = row.find('td', class_='td_name_param_phone')
                        if phone_label and phone_label.find_next_sibling('td'):
                            phone = phone_label.find_next_sibling('td').text.strip()
                            details['contact']['phone'] = self.clean_phone_number(phone)
                            break

                # Log the extracted details
                logger.debug(f"Extracted contact info: {details['contact']}")
                return details

        except Exception as e:
            logger.error(f"Error fetching listing details from {url}: {str(e)}")
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
        
        phone = details.get('contact', {}).get('phone')
        if not phone:
            logger.debug(f"No phone number found for listing {listing_data.get('id')}")
            return leads

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
        
        logger.debug(f"Created lead data: {lead_data}")
        leads.append(lead_data)
        return leads

    async def process_batch(self, session: aiohttp.ClientSession, listings: List[Dict]) -> None:
        """Process a batch of listings"""
        if not listings:
            return

        tasks = [self.fetch_listing_details(session, listing['link']) for listing in listings]
        details_list = await asyncio.gather(*tasks, return_exceptions=True)
        
        leads_batch = []
        for listing, details in zip(listings, details_list):
            try:
                if isinstance(details, Exception):
                    logger.error(f"Error fetching details for listing {listing.get('id')}: {str(details)}")
                    continue
                    
                if not isinstance(details, dict):
                    logger.error(f"Invalid details for listing {listing.get('id')}")
                    continue

                leads = self.extract_lead_data(listing, details)
                if leads:
                    leads_batch.extend(leads)
                    logger.info(f"Added {len(leads)} leads from listing {listing.get('id')}")

            except Exception as e:
                logger.error(f"Error processing listing {listing.get('id')}: {str(e)}")

        if leads_batch:
            try:
                logger.info(f"Saving batch of {len(leads_batch)} leads")
                self.db_manager.save_leads_batch(leads_batch)
                logger.info(f"Successfully saved {len(leads_batch)} leads")
            except Exception as e:
                logger.error(f"Error saving leads batch: {str(e)}")

    async def run_async(self):
        """Main scraping process"""
        async with aiohttp.ClientSession(headers=self.session.headers) as session:
            total_pages = await self.get_page_count(session)
            logger.info(f"Found {total_pages} total pages")
            
            all_listings = []
            page = 1
            consecutive_empty = 0
            
            while page <= total_pages and consecutive_empty < 3:
                try:
                    listings = await self.fetch_page(session, page)
                    
                    if not listings:
                        logger.warning(f"No listings found on page {page}")
                        consecutive_empty += 1
                    else:
                        consecutive_empty = 0
                        all_listings.extend(listings)
                        
                        # Process listings in small batches
                        batch_size = 5
                        for i in range(0, len(listings), batch_size):
                            batch = listings[i:i + batch_size]
                            await self.process_batch(session, batch)
                            await asyncio.sleep(1)  # Small delay between batches
                    
                except Exception as e:
                    logger.error(f"Error processing page {page}: {str(e)}")
                    consecutive_empty += 1
                
                if consecutive_empty >= 3:
                    logger.info("Three consecutive empty pages, stopping pagination")
                    break
                    
                page += 1
                await asyncio.sleep(0.5)  # Small delay between pages

            logger.info(f"Total listings processed: {len(all_listings)}")
            return all_listings

    def run(self):
        """Entry point for the scraper"""
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