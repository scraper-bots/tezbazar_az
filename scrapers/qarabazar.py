# scrapers/qarabazar.py
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
        """Print summary of scraping statistics"""
        print("\nScraping Statistics:")
        print(f"Total pages processed: {self.total_pages}")
        print(f"Total listings found: {self.total_listings}")
        print(f"Valid numbers found: {self.valid_numbers}")
        print(f"Invalid numbers found: {self.invalid_numbers}")
        if self.invalid_numbers > 0:
            print("\nInvalid phone numbers:")
            for phone in self.invalid_phone_list:
                print(f"  {phone}")

def extract_numbers_from_text(text: str) -> Set[str]:
    """Extract potential phone numbers from text using regex patterns"""
    patterns = [
        r'\+994\s*\d{2}\s*\d{3}\s*\d{2}\s*\d{2}',  # +994 XX XXX XX XX
        r'0\s*\d{2}\s*\d{3}\s*\d{2}\s*\d{2}',      # 0XX XXX XX XX
        r'\d{2}\s*\d{3}\s*\d{2}\s*\d{2}',          # XX XXX XX XX
    ]
    
    numbers = set()
    for pattern in patterns:
        matches = re.finditer(pattern, text, re.MULTILINE)
        numbers.update(match.group() for match in matches)
    
    return numbers

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
    """Get randomized headers"""
    user_agents = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ]
    
    return {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'az,en-US;q=0.7,en;q=0.3',
        'Connection': 'keep-alive',
        'User-Agent': random.choice(user_agents),
        'Referer': 'https://qarabazar.az'
    }

def make_request(session: requests.Session, url: str, max_retries: int = 3) -> Optional[BeautifulSoup]:
    """Make HTTP request with retries and random delays"""
    for attempt in range(max_retries):
        try:
            time.sleep(random.uniform(1, 2))
            response = session.get(url, headers=get_headers(), timeout=10)
            
            if response.status_code == 200:
                return BeautifulSoup(response.text, 'html.parser')
            elif response.status_code == 404:
                print(f"Page not found: {url}")
                return None
            elif response.status_code == 403:
                print(f"Access forbidden: {url}")
                time.sleep(random.uniform(5, 10))
            else:
                print(f"Got status code {response.status_code} for {url}")
            
        except requests.RequestException as e:
            print(f"Request error (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if attempt == max_retries - 1:
                raise
            time.sleep(random.uniform(2, 4))
    
    return None

def extract_phone_numbers(soup: BeautifulSoup) -> Set[str]:
    """Extract all possible phone numbers from the page"""
    phone_numbers = set()
    
    # Check for numbers in elements with itemprop="telephone"
    for elem in soup.find_all(['span', 'div', 'p', 'a'], {'itemprop': 'telephone'}):
        if elem.text:
            phone_numbers.add(''.join(elem.text.split()))
    
    # Check elements with common phone-related classes
    phone_classes = ['phone', 'phones', 'tel', 'telephone', 'contact']
    for class_name in phone_classes:
        for elem in soup.find_all(class_=lambda x: x and class_name in x.lower()):
            if elem.text:
                phone_numbers.add(''.join(elem.text.split()))
    
    # Look for phone patterns in href="tel:" attributes
    for elem in soup.find_all('a', href=re.compile(r'^tel:')):
        phone = elem['href'].replace('tel:', '')
        if phone:
            phone_numbers.add(''.join(phone.split()))
    
    # Check for numbers in the entire page text
    text_numbers = extract_numbers_from_text(soup.get_text())
    phone_numbers.update(text_numbers)
    
    return phone_numbers

def extract_listing_details(soup: BeautifulSoup, url: str, stats: ScraperStats) -> Optional[Dict]:
    """Extract details from a listing page"""
    try:
        details = {}

        # Extract title - try multiple possible elements
        for title_elem in soup.find_all(['h1', 'h2', 'h3', 'a'], class_=['title', 'title_synopsis_adv']):
            if title_elem.text.strip():
                details['title'] = title_elem.text.strip()
                break

        # Extract price - try multiple possible elements
        for price_elem in soup.find_all(['span', 'div'], class_=['price', 'value_cost_adv']):
            if price_elem.text.strip():
                details['price'] = price_elem.text.strip()
                break

        # Extract description - try multiple possible elements
        for desc_elem in soup.find_all(['div', 'p'], class_=['description', 'short-text-ads', 'details']):
            if desc_elem.text.strip():
                details['description'] = desc_elem.text.strip()
                break

        # Extract contact info
        contact_info = {'name': None}
        
        # Try to find seller name using schema.org markup
        seller_elem = soup.find(['div', 'span'], {'itemprop': 'seller'})
        if seller_elem:
            name_elem = seller_elem.find(['span', 'div'], {'itemprop': 'name'})
            if name_elem:
                contact_info['name'] = name_elem.text.strip()
        
        # If not found, try traditional class-based selectors
        if not contact_info['name']:
            contact_elem = soup.find(['div', 'section'], class_=['contact-info', 'seller-info'])
            if contact_elem:
                for name_elem in contact_elem.find_all(['div', 'span', 'p'], class_='name'):
                    if name_elem.text.strip():
                        contact_info['name'] = name_elem.text.strip()
                        break
        
        # Extract all possible phone numbers
        phone_numbers = extract_phone_numbers(soup)
        valid_phones = set()
        
        # Format and validate each phone number
        for phone in phone_numbers:
            formatted_phone = format_phone(phone, stats, phone)
            if formatted_phone:
                valid_phones.add(formatted_phone)
                stats.valid_numbers += 1
            else:
                stats.invalid_numbers += 1

        if not valid_phones:
            return None

        # Create base item structure
        base_item = {
            'name': contact_info['name'],
            'website': 'qarabazar.az',
            'link': url,
            'raw_data': {
                'title': details.get('title'),
                'price': details.get('price'),
                'description': details.get('description'),
                'all_phones': list(valid_phones)
            }
        }

        # Use first valid phone as primary
        base_item['phone'] = list(valid_phones)[0]
        
        return base_item
            
    except Exception as e:
        print(f"Error extracting listing details from {url}: {e}")
        return None

def get_listing_links(soup: BeautifulSoup, base_url: str) -> List[str]:
    """Extract all listing links from a page"""
    links = set()
    
    listing_classes = ['block_one_synopsis_advert', 'listing-item', 'item']
    for class_name in listing_classes:
        listings = soup.find_all(['div', 'article'], class_=class_name)
        for listing in listings:
            link_elem = None
            for elem in listing.find_all('a'):
                href = elem.get('href')
                if href and not href.startswith('#'):
                    link_elem = elem
                    break
                    
            if link_elem and link_elem.get('href'):
                link = urljoin(base_url, link_elem['href'])
                links.add(link)
                print(f"Found listing link: {link}")
    
    return list(links)

def scrape() -> List[Dict]:
    """Main scraping function"""
    session = requests.Session()
    base_url = "https://qarabazar.az/elanlar"
    items_to_process = []
    stats = ScraperStats()
    
    try:
        pages_to_scrape = [1, 2, 3]
        stats.total_pages = len(pages_to_scrape)
        print(f"Will scrape {len(pages_to_scrape)} pages")
        
        for page in pages_to_scrape:
            try:
                url = f"{base_url}/page{page}.html"
                print(f"\nProcessing page {page}/{len(pages_to_scrape)}")
                
                soup = make_request(session, url)
                if not soup:
                    print(f"Failed to get response for page {page}")
                    continue
                
                listing_links = get_listing_links(soup, base_url)
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
                            print(f"Successfully processed listing with phone {details['phone']}")
                        else:
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