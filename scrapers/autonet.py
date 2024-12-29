import logging
import time
from typing import Dict, List
from .base import BaseScraper
import concurrent.futures
import asyncio
import aiohttp

logger = logging.getLogger(__name__)

class AutonetScraper(BaseScraper):
    def __init__(self, base_url: str, db_manager):
        super().__init__(base_url, db_manager)
        self.api_url = "https://autonet.az/api/items/searchItem/"
        self.session.headers.update({
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'Authorization': 'Bearer null',
            'Connection': 'keep-alive',
            'Host': 'autonet.az',
            'Referer': 'https://autonet.az/items',
            'X-Authorization': '00028c2ddcc1ca6c32bc919dca64c288bf32ff2a',
            'X-Requested-With': 'XMLHttpRequest'
        })

    async def fetch_page(self, page: int) -> List[Dict]:
        params = {'page': page, 'limit': 24}
        
        async with aiohttp.ClientSession(headers=self.session.headers) as session:
            async with session.get(self.api_url, params=params) as response:
                data = await response.json()
                return data.get('data', [])

    async def fetch_all_pages(self, max_pages: int = 3) -> List[Dict]:
        tasks = []
        for page in range(1, max_pages + 1):
            tasks.append(self.fetch_page(page))
        
        all_listings = []
        results = await asyncio.gather(*tasks)
        for listings in results:
            all_listings.extend(listings)
        
        return all_listings

    def process_listing_batch(self, listings: List[Dict]):
        leads_batch = []
        for listing in listings:
            if not listing.get('phone1') and not listing.get('phone2'):
                continue
                
            try:
                leads = self.extract_lead_data(listing)
                leads_batch.extend(leads)
            except Exception as e:
                logger.error(f"Error processing listing {listing.get('id')}: {str(e)}")
        
        return leads_batch

    def extract_lead_data(self, listing_data: Dict) -> List[Dict]:
        item_url = f"https://autonet.az/items/view/{listing_data['id']}"
        leads = []
        phones = []
        
        # Process phone1
        if listing_data.get('phone1'):
            phone1 = listing_data['phone1'].replace('(', '').replace(')', '').replace(' ', '').replace('-', '')
            if not phone1.startswith('+'):
                phone1 = '+994' + phone1
            phones.append(phone1)
        
        # Process phone2 if different
        if listing_data.get('phone2') and listing_data['phone2'] != listing_data.get('phone1'):
            phone2 = listing_data['phone2'].replace('(', '').replace(')', '').replace(' ', '').replace('-', '')
            if not phone2.startswith('+'):
                phone2 = '+994' + phone2
            phones.append(phone2)
        
        base_data = {
            'name': listing_data.get('fullname', ''),
            'website': 'autonet.az',
            'link': item_url,
            'raw_data': {
                'make': listing_data.get('make', ''),
                'model': listing_data.get('model', ''),
                'year': listing_data.get('buraxilis_ili', ''),
                'price': listing_data.get('price', ''),
                'currency': listing_data.get('currency', ''),
                'city': listing_data.get('cityName', ''),
                'information': listing_data.get('information', ''),
                'engine_capacity': listing_data.get('engine_capacity', ''),
                'mileage': listing_data.get('yurus', ''),
                'listing_date': listing_data.get('created_at', ''),
                'id': listing_data.get('id', ''),
                'cover_image': listing_data.get('cover', ''),
                'item_type': listing_data.get('item_type', '')
            }
        }
        
        for phone in phones:
            lead_data = base_data.copy()
            lead_data['phone'] = phone
            leads.append(lead_data)
        
        return leads

    def run(self):
        try:
            # Get all listings asynchronously
            listings = asyncio.run(self.fetch_all_pages())
            logger.info(f"Found {len(listings)} total listings")
            
            # Process listings in parallel batches
            batch_size = 50
            listing_batches = [listings[i:i + batch_size] for i in range(0, len(listings), batch_size)]
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(self.process_listing_batch, batch) 
                          for batch in listing_batches]
                
                for future in concurrent.futures.as_completed(futures):
                    leads_batch = future.result()
                    if leads_batch:
                        self.db_manager.save_leads_batch(leads_batch)
            
            # Flush any remaining leads
            self.db_manager.flush_batch()
            
        except Exception as e:
            logger.error(f"Error in scraping process: {str(e)}")
            raise