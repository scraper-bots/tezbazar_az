import logging
import time
from typing import Dict, List
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
            'Upgrade-Insecure-Requests': '1',
        })
        
    async def fetch_page(self, page: int) -> List[Dict]:
        params = {
            'home_search': '1',
            'lang': '1',
            'site': '1',
            'home_s': '1',
            'price_min': '',
            'price_max': '',
            'axtar': '',
            'sahe_min': '',
            'sahe_max': '',
            'mertebe_min': '',
            'mertebe_max': '',
            'y_mertebe_min': '',
            'y_mertebe_max': '',
            'page': page
        }
        
        async with aiohttp.ClientSession(headers=self.session.headers) as session:
            async with session.get(self.search_url, params=params) as response:
                html = await response.text()
                return self.parse_listing_page(html)

    def parse_listing_page(self, html: str) -> List[Dict]:
        soup = BeautifulSoup(html, 'html.parser')
        listings = []
        
        for item in soup.select('li.new_elan_box'):
            try:
                listing = {
                    'id': item.get('id', '').replace('elan_', ''),
                    'link': self.base_url + item.select_one('a')['href'],
                    'title': item.select_one('.elan_property_title').text.strip(),
                    'price': self.extract_price(item.select_one('.elan_price').text),
                    'location': item.select_one('.elan_unvan').text.strip(),
                    'date': item.select_one('.elan_box_date').text.strip()
                }
                
                # Extract property details
                params = {}
                for row in item.select('.n_elan_box_botom_params tr td'):
                    text = row.text.strip()
                    if 'otaqlı' in text:
                        params['rooms'] = text.replace('otaqlı', '').strip()
                    elif 'm²' in text:
                        params['area'] = text.replace('m²', '').strip()
                    elif 'mərtəbə' in text:
                        params['floor'] = text.strip()
                
                listing['details'] = params
                listings.append(listing)
            except Exception as e:
                logger.error(f"Error parsing listing: {str(e)}")
                
        return listings

    async def fetch_listing_details(self, url: str) -> Dict:
        async with aiohttp.ClientSession(headers=self.session.headers) as session:
            async with session.get(url) as response:
                html = await response.text()
                return self.parse_listing_details(html)

    def parse_listing_details(self, html: str) -> Dict:
        soup = BeautifulSoup(html, 'html.parser')
        details = {}
        
        try:
            # Extract contact info
            user_info = soup.select_one('.new_elan_user_info')
            if user_info:
                details['name'] = user_info.select_one('p').text.strip()
                phones = []
                for phone in user_info.select('.elan_in_tel'):
                    phone_number = phone.text.strip()
                    if phone_number:
                        phones.append(self.clean_phone_number(phone_number))
                details['phones'] = phones

            # Extract property features
            features = []
            for feature in soup.select('.property_lists li'):
                features.append(feature.text.strip())
            details['features'] = features

            # Extract description
            description = soup.select_one('.elan_info_txt')
            if description:
                details['description'] = description.text.strip()

        except Exception as e:
            logger.error(f"Error parsing listing details: {str(e)}")

        return details

    def clean_phone_number(self, phone: str) -> str:
        """Clean and format phone number"""
        phone = ''.join(filter(str.isdigit, phone))
        if phone.startswith('0'):
            phone = '+994' + phone[1:]
        return phone

    def extract_price(self, price_text: str) -> Dict:
        """Extract price and currency from price text"""
        try:
            amount = ''.join(filter(str.isdigit, price_text))
            currency = 'AZN' if 'AZN' in price_text else ''
            return {
                'amount': int(amount) if amount else 0,
                'currency': currency
            }
        except Exception as e:
            logger.error(f"Error extracting price from {price_text}: {str(e)}")
            return {'amount': 0, 'currency': ''}

    async def process_listings(self, listings: List[Dict]) -> List[Dict]:
        """Process listings and fetch their details"""
        tasks = []
        for listing in listings:
            tasks.append(self.fetch_listing_details(listing['link']))
        
        details_list = await asyncio.gather(*tasks)
        
        processed_listings = []
        for listing, details in zip(listings, details_list):
            listing.update(details)
            processed_listings.append(listing)
        
        return processed_listings

    async def fetch_all_pages(self, max_pages: int = 3) -> List[Dict]:
        all_listings = []
        for page in range(1, max_pages + 1):
            try:
                listings = await self.fetch_page(page)
                if not listings:
                    break
                processed_listings = await self.process_listings(listings)
                all_listings.extend(processed_listings)
                logger.info(f"Processed page {page}, found {len(listings)} listings")
                await asyncio.sleep(2)  # Rate limiting
            except Exception as e:
                logger.error(f"Error processing page {page}: {str(e)}")
                
        return all_listings

    def extract_lead_data(self, listing_data: Dict) -> List[Dict]:
        leads = []
        
        for phone in listing_data.get('phones', []):
            lead_data = {
                'name': listing_data.get('name', ''),
                'phone': phone,
                'website': 'arenda.az',
                'link': listing_data.get('link', ''),
                'raw_data': {
                    'title': listing_data.get('title', ''),
                    'price': listing_data.get('price', {}),
                    'location': listing_data.get('location', ''),
                    'details': listing_data.get('details', {}),
                    'features': listing_data.get('features', []),
                    'description': listing_data.get('description', ''),
                    'date': listing_data.get('date', ''),
                    'id': listing_data.get('id', '')
                }
            }
            leads.append(lead_data)
            
        return leads

    def run(self):
        try:
            # Get all listings asynchronously
            listings = asyncio.run(self.fetch_all_pages())
            logger.info(f"Found {len(listings)} total listings")
            
            # Process listings and extract leads
            for listing in listings:
                try:
                    leads = self.extract_lead_data(listing)
                    if leads:
                        self.db_manager.save_leads_batch(leads)
                except Exception as e:
                    logger.error(f"Error processing listing {listing.get('id')}: {str(e)}")
            
            # Flush any remaining leads
            self.db_manager.flush_batch()
            
        except Exception as e:
            logger.error(f"Error in scraping process: {str(e)}")
            raise