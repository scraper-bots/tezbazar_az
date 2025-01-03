"""
Scraper module for birja-in.az real estate listings.
Extracts property details, contact information, and prices from listings.
"""
import requests
from bs4 import BeautifulSoup
import time
import random
import re
from typing import Dict, List, Optional
from dataclasses import dataclass
import json

@dataclass
class ScraperStats:
    total_items: int = 0
    valid_numbers: int = 0
    invalid_numbers: int = 0
    invalid_phone_list: List[str] = None

    def __post_init__(self):
        self.invalid_phone_list = []

    def print_stats(self):
        """Print scraping statistics"""
        print("\nScraping Statistics:")
        print(f"Total items processed: {self.total_items}")
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
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'User-Agent': random.choice(user_agents)
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

def extract_listing_details(soup: BeautifulSoup) -> Dict:
    """Extract details from a listing page"""
    details = {}
    
    # Extract title
    title_elem = soup.find('h1')
    if title_elem:
        details['title'] = title_elem.text.strip()
    
    # Extract price
    price_elem = soup.find('span', class_='value_cost_adv')
    if price_elem:
        details['price'] = price_elem.text.strip()
        
    # Extract location
    location_elem = soup.find('td', text=re.compile(r'Şəhər/ərazi'))
    if location_elem and location_elem.find_next('td'):
        details['location'] = location_elem.find_next('td').text.strip()
    
    # Extract phone number
    phone_elem = soup.find('td', class_='td_name_param_phone')
    if phone_elem and phone_elem.find_next('td'):
        details['phone'] = phone_elem.find_next('td').text.strip()
    
    # Extract description
    desc_elem = soup.find('td', class_='td_text_advert')
    if desc_elem:
        details['description'] = desc_elem.text.strip()
    
    # Extract contact person
    contact_elem = soup.find('td', class_='name_adder')
    if contact_elem:
        details['contact_name'] = contact_elem.text.strip()
    
    # Extract property details
    property_details = {}
    property_rows = soup.find_all('tr')
    for row in property_rows:
        param_cell = row.find('td', class_='td_name_param')
        if param_cell and row.find_all('td')[1:]:
            key = param_cell.text.strip()
            value = row.find_all('td')[1].text.strip()
            property_details[key] = value
    
    details['property_details'] = property_details
    
    return details

def get_listing_links(soup: BeautifulSoup) -> List[str]:
    """Extract all listing links from a page"""
    links = []
    listings = soup.find_all('div', class_='block_one_synopsis_advert')
    
    for listing in listings:
        link_elem = listing.find('a', class_='title_synopsis_adv')
        if link_elem and link_elem.get('href'):
            link = link_elem['href']
            if not link.startswith('http'):
                link = f"https://birja-in.az{link}"
            links.append(link)
    
    return links

def process_items(items: List[Dict], stats: ScraperStats) -> List[Dict]:
    """Process items and format them for database"""
    processed_items = []
    for item in items:
        phone = format_phone(item.get('phone'), stats, item.get('phone'))
        
        if phone:
            stats.valid_numbers += 1
            processed_item = {
                'name': item.get('contact_name'),
                'phone': phone,
                'website': 'birja-in.az',
                'link': item.get('link'),
                'raw_data': item
            }
            processed_items.append(processed_item)
            print(f"Successfully processed item with phone {phone}")
        else:
            stats.invalid_numbers += 1
    
    return processed_items

def scrape() -> List[Dict]:
    """Main scraping function"""
    session = requests.Session()
    base_url = "https://birja-in.az/elanlar"
    raw_items = []
    stats = ScraperStats()
    
    try:
        pages_to_scrape = [1, 2, 3]
        print(f"Will scrape pages: {pages_to_scrape}")
        
        for page in pages_to_scrape:
            try:
                url = f"{base_url}/page{page}.html" if page > 1 else base_url
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
                        
                        details = extract_listing_details(listing_soup)
                        details['link'] = link
                        
                        if details.get('phone'):
                            raw_items.append(details)
                            print(f"Successfully extracted details with phone {details['phone']}")
                        else:
                            print(f"No phone number found for listing {link}")
                        
                    except Exception as e:
                        print(f"Error processing listing {link}: {e}")
                        continue
                    
            except Exception as e:
                print(f"Error processing page {page}: {e}")
                continue

        # Process all collected items
        stats.total_items = len(raw_items)
        processed_items = process_items(raw_items, stats)
        
        # Print statistics
        stats.print_stats()
        
        return processed_items
        
    except Exception as e:
        print(f"Scraping error: {e}")
        return []