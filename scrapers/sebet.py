import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import time
import random
import re
from typing import Dict, List, Optional, Set
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
        print(f"Valid numbers found: {self.valid_numbers}")
        print(f"Invalid numbers found: {self.invalid_numbers}")
        if self.invalid_numbers > 0:
            print("\nInvalid phone numbers:")
            for phone in self.invalid_phone_list:
                print(f"  {phone}")

def format_phone(phone: str, stats: Optional[ScraperStats] = None, original: str = None) -> Optional[str]:
    """Format and validate phone number according to rules"""
    if not phone:
        return None
        
    # Remove all non-digit characters
    digits = re.sub(r'\D', '', phone)
    
    # Remove country code if present
    if digits.startswith('994'): 
        digits = digits[3:]
    if digits.startswith('0'): 
        digits = digits[1:]
    
    # Validate length
    if len(digits) != 9:
        if stats:
            stats.invalid_phone_list.append(f"Length error - Original: {original}, Cleaned: {digits}")
        return None
    
    # Validate prefix
    valid_prefixes = ('10', '12', '50', '51', '55', '60', '70', '77', '99')
    if not digits.startswith(valid_prefixes):
        if stats:
            stats.invalid_phone_list.append(f"Prefix error - Original: {original}, Cleaned: {digits}")
        return None
    
    # Validate fourth digit
    if digits[3] in ('0', '1'):
        if stats:
            stats.invalid_phone_list.append(f"Fourth digit error - Original: {original}, Cleaned: {digits}")
        return None
        
    return digits

def get_headers() -> Dict[str, str]:
    """Get randomized headers"""
    user_agents = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ]
    
    return {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'az,en-US;q=0.7,en;q=0.3',
        'Connection': 'keep-alive',
        'User-Agent': random.choice(user_agents),
        'Referer': 'https://sebet.az'
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
            print(f"Request error (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(random.uniform(2, 4))
    
    return None

def extract_phone_from_link(href: str) -> Optional[str]:
    """Extract phone number from tel: link"""
    if not href:
        return None
    phone_match = re.search(r'tel:\s*(\d+)', href)
    return phone_match.group(1) if phone_match else None

def extract_listing_details(soup: BeautifulSoup, url: str, stats: ScraperStats) -> Optional[Dict]:
    """Extract details from a listing page"""
    try:
        # Extract phone number from tel: link
        phone_elem = soup.find('a', href=lambda x: x and x.startswith('tel:'))
        if not phone_elem:
            return None
            
        raw_phone = extract_phone_from_link(phone_elem['href'])
        if not raw_phone:
            return None
            
        formatted_phone = format_phone(raw_phone, stats, raw_phone)
        if not formatted_phone:
            return None

        # Extract title
        title = None
        title_elem = soup.find('h1', class_='prodname')
        if title_elem:
            title = title_elem.text.strip()

        # Extract price
        price = None
        price_elem = soup.find('span', class_='sprice')
        if price_elem:
            price = price_elem.text.strip()

        # Extract product code
        product_code = None
        code_elem = soup.find('span', class_='id')
        if code_elem:
            product_code = code_elem.text.strip()

        # Get seller name (if available)
        seller_name = "Sebet.az"  # Default to website name since individual seller names aren't shown

        return {
            'name': seller_name,
            'phone': formatted_phone,
            'website': 'sebet.az',
            'link': url,
            'raw_data': {
                'title': title,
                'price': price,
                'product_code': product_code
            }
        }

    except Exception as e:
        print(f"Error extracting listing details: {e}")
        return None

def get_listing_links(soup: BeautifulSoup) -> List[str]:
    """Extract all listing links from a page"""
    links = []
    products = soup.find_all('div', class_='nobj prod')
    
    for product in products:
        link_elem = product.find('a')
        if link_elem and link_elem.get('href'):
            link = link_elem['href']
            if not link.startswith('http'):
                link = urljoin('https://sebet.az', link)
            links.append(link)
    
    return links

def scrape() -> List[Dict]:
    """Main scraping function"""
    session = requests.Session()
    base_url = "https://sebet.az/homelist"
    items_to_process = []
    stats = ScraperStats()
    
    try:
        pages_to_scrape = [1, 2, 3]  # Default to first 3 pages
        stats.total_pages = len(pages_to_scrape)
        print(f"Will scrape {len(pages_to_scrape)} pages")
        
        for page in pages_to_scrape:
            try:
                url = f"{base_url}/{page}"
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
                        
                        details = extract_listing_details(listing_soup, link, stats)
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
        results = scrape()
        print(f"\nScraping completed. Found {len(results)} valid listings.")
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
    except Exception as e:
        print(f"\nError during scraping: {e}")
        raise