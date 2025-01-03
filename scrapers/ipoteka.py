# scrapers/ipoteka.py
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import time
import random
import re
from typing import Dict, List, Optional
from dataclasses import dataclass
from urllib.parse import urljoin

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
        """Print scraping statistics summary"""
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
    """Format and validate phone number according to rules"""
    if not phone:
        return None
    
    digits = re.sub(r'\D', '', phone)
    
    if digits.startswith('994'):
        digits = digits[3:]
    if digits.startswith('0'):
        digits = digits[1:]
    
    if len(digits) != 9:
        if stats:
            stats.invalid_phone_list.append(f"Length error - Original: {original}, Cleaned: {digits}")
        return None
    
    valid_prefixes = ('10', '12', '50', '51', '55', '60', '70', '77', '99')
    if not digits.startswith(valid_prefixes):
        if stats:
            stats.invalid_phone_list.append(f"Prefix error - Original: {original}, Cleaned: {digits}")
        return None
    
    if digits[3] in ('0', '1'):
        if stats:
            stats.invalid_phone_list.append(f"Fourth digit error - Original: {original}, Cleaned: {digits}")
        return None
    
    return digits

def get_headers() -> Dict[str, str]:
    """Get randomized headers for requests"""
    user_agents = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ]
    
    return {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'az,en-US;q=0.7,en;q=0.3',
        'Connection': 'keep-alive',
        'User-Agent': random.choice(user_agents),
        'Referer': 'https://ipoteka.az'
    }

def make_request(session: requests.Session, url: str, max_retries: int = 3) -> Optional[BeautifulSoup]:
    """Make HTTP request with retries and random delays"""
    for attempt in range(max_retries):
        try:
            time.sleep(random.uniform(1, 2))
            response = session.get(url, headers=get_headers(), timeout=10)
            
            if response.status_code == 200:
                return BeautifulSoup(response.text, 'html.parser')
            
            print(f"Got status code {response.status_code} for {url}")
            
        except Exception as e:
            print(f"Request error (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if attempt == max_retries - 1:
                raise
            time.sleep(random.uniform(2, 4))
    
    return None

def extract_listing_details(soup: BeautifulSoup, url: str) -> List[Dict]:
    """Extract details from a listing page"""
    try:
        items = []
        
        # Extract title
        title = None
        title_elem = soup.find('h2', class_='title')
        if title_elem:
            title = title_elem.text.strip()
        
        # Extract price
        price = None
        price_elem = soup.find('span', class_='price')
        if price_elem:
            price = price_elem.text.strip()
        
        # Extract description
        description = None
        desc_elem = soup.find('div', class_='text')
        if desc_elem:
            description = desc_elem.text.strip()
        
        # Extract contact info
        contact_name = None
        user_elem = soup.find('div', class_='user')
        if user_elem:
            contact_name = user_elem.text.strip()
        
        # Extract technical characteristics
        tech_chars = {}
        params_block = soup.find('div', class_='params_block')
        if params_block:
            for row in params_block.find_all('div', class_='rw'):
                cells = row.find_all('div')
                if len(cells) >= 2:
                    key = cells[0].text.strip()
                    value = cells[1].text.strip()
                    tech_chars[key] = value
        
        # Extract phone numbers
        phone_numbers = []
        links = soup.find('ul', {'class': 'links', 'style': lambda x: x and '#263f58' in x if x else False})
        if links:
            for div in links.find_all('div', class_='active'):
                phone = div.get('number', '')
                if not phone and div.text:
                    phone = div.text.strip()
                if phone:
                    phone = re.sub(r'\s+', '', phone)
                    phone = re.sub(r'[^\d+]', '', phone)
                    phone_numbers.append(phone)
        
        # Create an item for each phone number
        for phone in phone_numbers:
            formatted_phone = format_phone(phone, None, phone)
            if formatted_phone:
                item = {
                    'name': contact_name,
                    'phone': formatted_phone,
                    'website': 'ipoteka.az',
                    'link': url,
                    'raw_data': {
                        'title': title,
                        'price': price,
                        'description': description,
                        'technical_characteristics': tech_chars
                    }
                }
                items.append(item)
        
        return items
        
    except Exception as e:
        print(f"Error extracting listing details: {e}")
        return []

def get_listing_links(soup: BeautifulSoup, base_url: str = 'https://ipoteka.az') -> List[str]:
    """Extract all listing links from a page"""
    links = []
    listings = soup.find_all('a', class_='item')
    
    for listing in listings:
        href = listing.get('href')
        if href:
            full_url = urljoin(base_url, href)
            links.append(full_url)
    
    return links

def scrape() -> List[Dict]:
    """Main scraping function"""
    session = requests.Session()
    base_url = "https://ipoteka.az/search"
    items_to_process = []
    stats = ScraperStats()
    
    try:
        pages_to_scrape = [1, 2, 3]
        stats.total_pages = len(pages_to_scrape)
        print(f"Will scrape {len(pages_to_scrape)} pages")
        
        for page in pages_to_scrape:
            try:
                url = f"{base_url}?ad_type=0&search_type=1&page={page}"
                print(f"\nProcessing page {page}/{len(pages_to_scrape)}")
                
                soup = make_request(session, url)
                if not soup:
                    print(f"Failed to get response for page {page}")
                    continue
                
                listing_links = get_listing_links(soup)
                print(f"Found {len(listing_links)} listings on page {page}")
                
                for idx, link in enumerate(listing_links, 1):
                    try:
                        print(f"Processing listing {idx}/{len(listing_links)}: {link}")
                        
                        listing_soup = make_request(session, link)
                        if not listing_soup:
                            print(f"Failed to get listing details for {link}")
                            continue
                        
                        items = extract_listing_details(listing_soup, link)
                        if items:
                            items_to_process.extend(items)
                            stats.valid_numbers += len(items)
                            print(f"Successfully processed listing with {len(items)} phone numbers")
                        else:
                            stats.invalid_numbers += 1
                            print(f"No valid phone numbers found for listing {link}")
                            
                        stats.total_listings += 1
                        
                    except Exception as e:
                        print(f"Error processing listing {link}: {e}")
                        continue
                    
            except Exception as e:
                print(f"Error processing page {page}: {e}")
                continue

        # Print final statistics
        stats.print_summary()
        
    except Exception as e:
        print(f"Scraping error: {e}")
    
    return items_to_process