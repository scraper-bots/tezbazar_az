import aiohttp
from bs4 import BeautifulSoup
import re
from datetime import datetime
import logging
import json
from typing import List, Dict, Optional
import asyncio
from fake_useragent import UserAgent

logger = logging.getLogger(__name__)

async def fetch_page(session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore) -> Optional[str]:
    headers = {'User-Agent': UserAgent().random}
    async with semaphore:
        try:
            async with session.get(url, headers=headers, timeout=30) as response:
                if response.status == 200:
                    return await response.text()
                logger.error(f"HTTP {response.status} for {url}")
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
    return None

def extract_phone(phone_raw: str) -> Optional[str]:
    """Extract and format phone number to XXXXXXXXX format"""
    match = re.search(r'\(0(\d{2})\)\s*(\d{3})(\d{4})', phone_raw)
    if match:
        area, first, last = match.groups()
        number = f"{area}{first}{last}"
        if validate_phone(number):
            return number
    return None

def validate_phone(phone: str) -> bool:
    """Validate phone number according to Azerbaijan rules"""
    if len(phone) != 9:
        return False
        
    valid_prefixes = {'10', '12', '50', '51', '55', '60', '70', '77', '99'}
    if phone[:2] not in valid_prefixes:
        return False
        
    if phone[2] in ['0', '1']:
        return False
        
    return True

def parse_listing(soup: BeautifulSoup, url: str) -> Optional[Dict]:
    """Parse individual listing data"""
    try:
        # Extract seller name
        name = None
        user_elem = soup.select_one('.infocontact .glyphicon-user')
        if user_elem and user_elem.next_sibling:
            name = user_elem.next_sibling.text.strip()
            if '(Bütün Elanları)' in name:
                name = name.replace('(Bütün Elanları)', '').strip()

        # Extract phone number
        telzona = soup.find('div', class_='telzona')
        if not telzona:
            return None

        phone_div = telzona.find('div', id='telshow')
        if not phone_div:
            return None

        phone_text = phone_div.text.strip()
        phone = extract_phone(phone_text)
        if not phone:
            return None

        # Extract listing details
        details = {}
        for p in soup.select("#openhalf p"):
            if b := p.find('b'):
                key = b.text.strip()
                value = p.text.replace(key, '').strip()
                details[key] = value

        return {
            "name": name,
            "phone": phone,
            "website": "unvan.az",
            "link": url,
            "scraped_at": datetime.now(),
            "raw_data": json.dumps({
                "details": details,
                "html": str(soup),
                "scrape_date": datetime.now().isoformat()
            })
        }
        
    except Exception as e:
        logger.error(f"Error parsing listing: {e}")
        return None

def scrape() -> List[Dict]:
    """Main scraping function"""
    async def run():
        results = []
        base_url = "https://unvan.az"
        semaphore = asyncio.Semaphore(10)
        
        async with aiohttp.ClientSession() as session:
            # Collect detail page URLs
            detail_urls = []
            for page in range(1, 3):
                list_url = f"{base_url}/avtomobil?start={page}"
                html = await fetch_page(session, list_url, semaphore)
                if not html:
                    continue

                soup = BeautifulSoup(html, 'html.parser')
                for link in soup.find_all('a', href=re.compile(r'/[^/]+-\d{6}\.html')):
                    href = link.get('href')
                    if href and href not in detail_urls:
                        detail_urls.append(href)

            # Process detail pages in batches
            for i in range(0, len(detail_urls), 10):
                batch = detail_urls[i:i+10]
                for url in batch:
                    full_url = f"{base_url}{url}"
                    html = await fetch_page(session, full_url, semaphore)
                    if html:
                        soup = BeautifulSoup(html, 'html.parser')
                        if listing := parse_listing(soup, full_url):
                            results.append(listing)
                await asyncio.sleep(0.1)

        return results

    return asyncio.run(run())