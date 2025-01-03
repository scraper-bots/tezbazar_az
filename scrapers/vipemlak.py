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
        'Referer': 'https://vipemlak.az'
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

def extract_listing_details(soup: BeautifulSoup, url: str) -> Optional[Dict]:
    """Extract details from a listing page"""
    try:
        details = {}
        
        # Extract contact info and phone number
        contact_info = soup.find('div', class_='infocontact')
        if not contact_info:
            print(f"No contact info found for {url}")
            return None
            
        # Extract name
        name = None
        user_elem = contact_info.find('span', class_='glyphicon-user')
        if user_elem and user_elem.find_next('a'):
            name = user_elem.find_next('a').text.strip()
            if '(Bütün Elanları)' in name:
                name = name.replace('(Bütün Elanları)', '').strip()

        # Extract phone number
        phone = None
        tel_zone = contact_info.find('div', class_='telzona')
        if tel_zone:
            tel_div = tel_zone.find('div', id='telshow')
            if tel_div:
                phone = tel_div.text.strip()
                print(f"Found raw phone: {phone}")
                cleaned_phone = re.sub(r'\D', '', phone)
                print(f"Cleaned phone: {cleaned_phone}")
                if cleaned_phone.startswith('994'):
                    cleaned_phone = cleaned_phone[3:]
                if cleaned_phone.startswith('0'):
                    cleaned_phone = cleaned_phone[1:]
                print(f"Final cleaned phone: {cleaned_phone}")
            else:
                print(f"No telshow div found in telzona for {url}")
        else:
            print(f"No telzona div found for {url}")
            
        if not phone:
            print(f"No phone number found for {url}")
            return None
            
        formatted_phone = format_phone(phone)
        if not formatted_phone:
            return None

        # Extract title
        title = None
        title_elem = soup.find('h1', class_='fs-24')
        if title_elem and title_elem.find('strong'):
            title = title_elem.find('strong').text.strip()

        # Extract price
        price = None
        price_elem = soup.find('span', class_='pricecolor')
        if price_elem:
            price = price_elem.text.strip()

        # Extract description
        description = None
        info_div = soup.find('div', class_='infotd100')
        if info_div:
            description = info_div.text.strip()

        # Extract address
        address = None
        address_div = soup.find('div', class_='infotd100', string=lambda x: x and 'Ünvan:' in x)
        if address_div:
            address = address_div.text.replace('Ünvan:', '').strip()

        return {
            'name': name,
            'phone': formatted_phone,
            'website': 'vipemlak.az',
            'link': url,
            'raw_data': {
                'title': title,
                'price': price,
                'description': description,
                'address': address
            }
        }

    except Exception as e:
        print(f"Error extracting listing details: {e}")
        return None

def get_listing_links(soup: BeautifulSoup) -> List[str]:
    """Extract all listing links from a page"""
    links = []
    listings = soup.find_all('div', class_='pranto')
    
    for listing in listings:
        link_elem = listing.find('a')
        if link_elem and link_elem.get('href'):
            href = link_elem['href']
            if not href.startswith('http'):
                href = urljoin('https://vipemlak.az', href)
            links.append(href)
    
    return links

def scrape() -> List[Dict]:
    """Main scraping function"""
    session = requests.Session()
    base_url = "https://vipemlak.az/yeni-tikili"
    items_to_process = []
    stats = ScraperStats()
    
    try:
        pages_to_scrape = [1, 2, 3]  # Testing with first 3 pages
        stats.total_pages = len(pages_to_scrape)
        print(f"Will scrape {len(pages_to_scrape)} pages")
        
        for page in pages_to_scrape:
            try:
                url = f"{base_url}/?start={page}"
                print(f"\nProcessing page {page}/{len(pages_to_scrape)}")
                
                soup = make_request(session, url)
                if not soup:
                    print(f"Failed to get response for page {url}")
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
                        
                        details = extract_listing_details(listing_soup, link)
                        if details:
                            items_to_process.append(details)
                            stats.valid_numbers += 1
                            print(f"Successfully processed listing with phone {details['phone']}")
                        else:
                            stats.invalid_numbers += 1
                            print(f"No valid phone number found for listing {link}")
                            
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

if __name__ == "__main__":
    try:
        items = scrape()
        print(f"\nScraping completed. Found {len(items)} items.")
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
    except Exception as e:
        print(f"\nError during scraping: {e}")
        raise