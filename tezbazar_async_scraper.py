#!/usr/bin/env python3
"""
High-Performance Async Tezbazar.az Real Estate Scraper
Uses asyncio and aiohttp for maximum speed and concurrency
"""

import asyncio
import aiohttp
import aiofiles
from bs4 import BeautifulSoup
import json
import re
import time
from urllib.parse import urljoin
import pandas as pd
from typing import Dict, List, Optional, Set
import logging
from dataclasses import dataclass, asdict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class Listing:
    """Data class for real estate listing"""
    url: str
    listing_id: str
    title: str
    price: str
    location: str
    description: str
    category: str
    phone: str
    seller_name: str
    date_posted: str
    images: List[str]
    room_count: str = ""
    area: str = ""
    floor: str = ""

class AsyncTebazarScraper:
    """High-performance async scraper for tezbazar.az"""
    
    def __init__(self, max_concurrent: int = 10, request_delay: float = 0.5):
        self.base_url = "https://tezbazar.az"
        self.listings_url = "https://tezbazar.az/dasinmaz-emlak-ev-elanlari"
        self.ajax_url = "https://tezbazar.az/ajax.php"
        
        # Concurrency settings
        self.max_concurrent = max_concurrent
        self.request_delay = request_delay
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        # Headers for requests
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8,ru;q=0.7,az;q=0.6',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive'
        }
        
        # AJAX headers
        self.ajax_headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            'Origin': self.base_url
        }
        
        self.scraped_listings: List[Listing] = []
        self.processed_urls: Set[str] = set()
        
    async def create_session(self) -> aiohttp.ClientSession:
        """Create aiohttp session with proper configuration"""
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=20,
            ttl_dns_cache=300,
            use_dns_cache=True,
        )
        
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        
        return aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=self.headers
        )
    
    async def fetch_page(self, session: aiohttp.ClientSession, url: str, retries: int = 3) -> Optional[str]:
        """Fetch a page with retry logic and rate limiting"""
        async with self.semaphore:
            for attempt in range(retries):
                try:
                    await asyncio.sleep(self.request_delay * attempt)
                    
                    async with session.get(url) as response:
                        if response.status == 200:
                            return await response.text()
                        else:
                            logger.warning(f"HTTP {response.status} for {url}")
                            
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout for {url} (attempt {attempt + 1})")
                except Exception as e:
                    logger.error(f"Error fetching {url}: {e}")
                
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt)
            
            logger.error(f"Failed to fetch {url} after {retries} attempts")
            return None
    
    async def extract_listing_urls(self, session: aiohttp.ClientSession, page_start: int = 0) -> List[str]:
        """Extract listing URLs from a page"""
        if page_start == 0:
            url = self.listings_url
        else:
            url = f"{self.listings_url}/?start={page_start}"
        
        logger.info(f"Extracting URLs from: {url}")
        
        html_content = await self.fetch_page(session, url)
        if not html_content:
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        urls = []
        
        # Find all product containers
        product_containers = soup.find_all('div', class_='nobj')
        
        for container in product_containers:
            prodname_div = container.find('div', class_='prodname')
            if prodname_div:
                link = prodname_div.find('a', href=True)
                if link and link['href'].endswith('.html'):
                    full_url = urljoin(self.base_url, link['href'])
                    if full_url not in self.processed_urls:
                        urls.append(full_url)
        
        logger.info(f"Found {len(urls)} new listing URLs on page")
        return urls
    
    def extract_listing_id(self, url: str) -> str:
        """Extract listing ID from URL"""
        match = re.search(r'-(\d+)\.html$', url)
        return match.group(1) if match else ""
    
    def find_hash_value(self, page_content: str, listing_id: str) -> Optional[str]:
        """Find hash value for AJAX call"""
        patterns = [
            r'"h"\s*:\s*"([a-f0-9]{32})"',
            r"'h'\s*:\s*'([a-f0-9]{32})'",
            r'h\s*=\s*["\']([a-f0-9]{32})["\']',
            r'hash["\']?\s*[=:]\s*["\']([a-f0-9]{32})["\']'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, page_content)
            if match:
                return match.group(1)
        
        # Fallback: look for any 32-char hex string near tel content
        hex_matches = re.findall(r'([a-f0-9]{32})', page_content)
        for hex_val in hex_matches:
            context_start = max(0, page_content.find(hex_val) - 100)
            context_end = min(len(page_content), page_content.find(hex_val) + 100)
            context = page_content[context_start:context_end].lower()
            if any(keyword in context for keyword in ['tel', 'phone', 'ajax']):
                return hex_val
        
        return None
    
    async def get_phone_number(self, session: aiohttp.ClientSession, listing_id: str, hash_value: str, referer: str) -> Optional[str]:
        """Get phone number via AJAX call"""
        payload = {
            'act': 'telshow',
            'id': listing_id,
            't': 'product',
            'h': hash_value,
            'rf': 'dasinmaz-emlak-ev-elanlari'
        }
        
        try:
            headers = {**self.ajax_headers, 'Referer': referer}
            
            async with session.post(self.ajax_url, data=payload, headers=headers) as response:
                if response.status == 200:
                    try:
                        result = await response.json()
                        return result.get('tel')
                    except json.JSONDecodeError:
                        text_response = await response.text()
                        logger.warning(f"Invalid JSON response: {text_response[:200]}")
                        return None
                else:
                    logger.warning(f"AJAX request failed: {response.status}")
                    return None
                    
        except Exception as e:
            logger.error(f"AJAX error for listing {listing_id}: {e}")
            return None
    
    async def parse_listing(self, session: aiohttp.ClientSession, listing_url: str) -> Optional[Listing]:
        """Parse individual listing page"""
        if listing_url in self.processed_urls:
            return None
        
        self.processed_urls.add(listing_url)
        logger.info(f"Parsing: {listing_url}")
        
        html_content = await self.fetch_page(session, listing_url)
        if not html_content:
            return None
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Initialize listing data
        listing_id = self.extract_listing_id(listing_url)
        
        listing = Listing(
            url=listing_url,
            listing_id=listing_id,
            title="",
            price="",
            location="",
            description="",
            category="",
            phone="",
            seller_name="",
            date_posted="",
            images=[]
        )
        
        # Extract title
        title_elem = soup.find('h1')
        if title_elem:
            listing.title = title_elem.get_text(strip=True)
        
        # Extract listing ID from page if not found in URL
        if not listing.listing_id:
            code_elem = soup.find('span', class_='open_idshow')
            if code_elem:
                id_match = re.search(r'(\d+)', code_elem.get_text())
                if id_match:
                    listing.listing_id = id_match.group(1)
        
        # Extract price
        price_elem = soup.find('span', class_='pricecolor')
        if price_elem:
            listing.price = price_elem.get_text(strip=True)
        
        # Extract description
        desc_elem = soup.find('p', class_='infop100')
        if desc_elem:
            desc_text = desc_elem.get_text(strip=True)
            listing.description = desc_text
            
            # Extract structured details from description
            room_match = re.search(r'Otaq sayı:\s*(\d+)', desc_text)
            if room_match:
                listing.room_count = room_match.group(1)
            
            area_match = re.search(r'Sahəsi:\s*([\d.,]+\s*kv\.?m?\.?)', desc_text)
            if area_match:
                listing.area = area_match.group(1)
            
            floor_match = re.search(r'Mərtəbə:\s*([\d/]+)', desc_text)
            if floor_match:
                listing.floor = floor_match.group(1)
        
        # Extract contact info
        contact_div = soup.find('div', class_='infocontact')
        if contact_div:
            # Seller name
            seller_link = contact_div.find('a', href=lambda x: x and '/user/' in x)
            if seller_link:
                listing.seller_name = seller_link.get_text(strip=True).split('(')[0].strip()
            
            # Location
            location_icon = contact_div.find('span', class_='glyphicon-map-marker')
            if location_icon and location_icon.parent:
                listing.location = location_icon.parent.get_text(strip=True)
        
        # Extract category
        breadcrumb = soup.find('div', class_='breadcrumb2')
        if breadcrumb:
            links = breadcrumb.find_all('a')
            if len(links) > 1:
                listing.category = links[-1].get_text(strip=True)
        
        # Extract date
        date_elem = soup.find('span', class_='viewsbb')
        if date_elem:
            listing.date_posted = date_elem.get_text(strip=True).replace('Tarix: ', '')
        
        # Extract images
        pic_area = soup.find('div', id='picsopen')
        if pic_area:
            for link in pic_area.find_all('a', href=True):
                href = link.get('href', '')
                if '/uploads/' in href:
                    listing.images.append(urljoin(self.base_url, href))
        
        # Try to get phone number
        phone_found = False
        
        # Check if phone is already visible
        tel_zone = soup.find('div', class_='telzona')
        if tel_zone and tel_zone.get('tel'):
            listing.phone = tel_zone.get('tel')
            phone_found = True
        
        # Try AJAX approach if needed
        if not phone_found and listing.listing_id:
            hash_value = self.find_hash_value(html_content, listing.listing_id)
            if hash_value:
                phone = await self.get_phone_number(session, listing.listing_id, hash_value, listing_url)
                if phone:
                    listing.phone = phone
                    phone_found = True
        
        # Fallback: look for phone patterns in page
        if not phone_found:
            phone_patterns = [
                r'\((\d{3})\)\s*(\d{7})',
                r'(\d{10})',
                r'0(\d{2})\s*(\d{7})'
            ]
            
            for pattern in phone_patterns:
                matches = re.findall(pattern, html_content)
                if matches:
                    if isinstance(matches[0], tuple):
                        listing.phone = ''.join(matches[0])
                    else:
                        listing.phone = matches[0]
                    break
        
        logger.info(f"✅ Parsed: {listing.title[:50]}... | Phone: {'✓' if listing.phone else '✗'}")
        return listing
    
    async def scrape_page_listings(self, session: aiohttp.ClientSession, page_start: int) -> List[Listing]:
        """Scrape all listings from a single page"""
        listing_urls = await self.extract_listing_urls(session, page_start)
        
        if not listing_urls:
            return []
        
        # Create tasks for concurrent processing
        tasks = []
        for url in listing_urls:
            task = asyncio.create_task(self.parse_listing(session, url))
            tasks.append(task)
        
        # Process listings concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter successful results
        listings = []
        for result in results:
            if isinstance(result, Listing):
                listings.append(result)
            elif isinstance(result, Exception):
                logger.error(f"Task failed: {result}")
        
        return listings
    
    async def scrape_all_pages(self, max_pages: int = None, max_listings: int = None) -> None:
        """Scrape all pages concurrently"""
        logger.info("🚀 Starting async scraping...")
        start_time = time.time()
        
        session = await self.create_session()
        try:
            page_start = 0
            page_count = 0
            
            while True:
                if max_pages and page_count >= max_pages:
                    logger.info(f"🛑 Reached max pages: {max_pages}")
                    break
                
                if max_listings and len(self.scraped_listings) >= max_listings:
                    logger.info(f"🛑 Reached max listings: {max_listings}")
                    break
                
                # Scrape current page
                page_listings = await self.scrape_page_listings(session, page_start)
                
                if not page_listings:
                    logger.info("🏁 No more listings found")
                    break
                
                # Add to results
                for listing in page_listings:
                    if max_listings and len(self.scraped_listings) >= max_listings:
                        break
                    self.scraped_listings.append(listing)
                
                page_count += 1
                page_start += 3  # Pagination increment
                
                logger.info(f"📄 Page {page_count} completed. Total: {len(self.scraped_listings)} listings")
                
                # Small delay between pages
                await asyncio.sleep(1)
        
        finally:
            await session.close()
        
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info(f"🎉 Scraping completed!")
        logger.info(f"📊 Total listings: {len(self.scraped_listings)}")
        logger.info(f"⏱️ Time taken: {duration:.2f} seconds")
        
        if self.scraped_listings:
            phone_count = sum(1 for listing in self.scraped_listings if listing.phone)
            success_rate = (phone_count / len(self.scraped_listings)) * 100
            logger.info(f"📞 Phone extraction: {phone_count}/{len(self.scraped_listings)} ({success_rate:.1f}%)")
            logger.info(f"🚀 Speed: {len(self.scraped_listings) / duration:.2f} listings/second")
    
    async def save_data(self, filename_base: str = 'tezbazar_async') -> None:
        """Save scraped data to files"""
        if not self.scraped_listings:
            logger.warning("No data to save")
            return
        
        # Save to JSON
        json_file = f"{filename_base}.json"
        json_data = [asdict(listing) for listing in self.scraped_listings]
        
        async with aiofiles.open(json_file, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(json_data, ensure_ascii=False, indent=2))
        
        logger.info(f"💾 Saved to {json_file}")
        
        # Save to CSV
        csv_file = f"{filename_base}.csv"
        csv_data = []
        
        for listing in self.scraped_listings:
            csv_data.append({
                'listing_id': listing.listing_id,
                'title': listing.title,
                'price': listing.price,
                'location': listing.location,
                'category': listing.category,
                'room_count': listing.room_count,
                'area': listing.area,
                'floor': listing.floor,
                'phone': listing.phone,
                'seller_name': listing.seller_name,
                'date_posted': listing.date_posted,
                'description': listing.description[:500],  # Truncate for CSV
                'image_count': len(listing.images),
                'url': listing.url
            })
        
        df = pd.DataFrame(csv_data)
        df.to_csv(csv_file, index=False, encoding='utf-8')
        logger.info(f"💾 Saved to {csv_file}")


async def main():
    """Main function"""
    print("🏠 Tezbazar.az High-Performance Async Scraper")
    print("=" * 50)
    
    # Configuration
    max_concurrent = 15  # Adjust based on your needs
    max_listings = int(input("Enter max listings to scrape (default 100): ") or "100")
    
    # Create scraper
    scraper = AsyncTebazarScraper(
        max_concurrent=max_concurrent,
        request_delay=0.3  # Small delay to be respectful
    )
    
    print(f"🚀 Scraping up to {max_listings} listings with {max_concurrent} concurrent connections...")
    
    # Start scraping
    await scraper.scrape_all_pages(max_listings=max_listings)
    
    # Save results
    await scraper.save_data('tezbazar_async_results')
    
    # Display sample
    if scraper.scraped_listings:
        print(f"\n📋 Sample listing:")
        sample = scraper.scraped_listings[0]
        print(f"   🏠 Title: {sample.title}")
        print(f"   💰 Price: {sample.price}")
        print(f"   📞 Phone: {sample.phone}")
        print(f"   🏷️ Category: {sample.category}")
        print(f"   🔗 URL: {sample.url}")


if __name__ == "__main__":
    asyncio.run(main())