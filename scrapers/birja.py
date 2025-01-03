import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import time
import random
import re
import concurrent.futures
from dataclasses import dataclass
from typing import Dict, List, Optional
from urllib.parse import urljoin

@dataclass
class ScraperStats:
    total_categories: int = 0
    total_listings: int = 0
    valid_numbers: int = 0
    invalid_numbers: int = 0
    invalid_phone_list: List[str] = None

    def __post_init__(self):
        self.invalid_phone_list = []

    def print_stats(self):
        """Print scraping statistics"""
        print("\nScraping Statistics:")
        print(f"Total categories processed: {self.total_categories}")
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
    
    if not digits.startswith(('10', '12', '50', '51', '55', '60', '70', '77', '99')):
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
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ]
    return {
        'User-Agent': random.choice(user_agents),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive'
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

def get_categories(session: requests.Session) -> List[Dict]:
    """Get all category links from the main categories page"""
    base_url = "https://birja.com/all_category/az"
    categories = []
    
    try:
        soup = make_request(session, base_url)
        if not soup:
            return categories
            
        for section in soup.find_all('div', class_='col-md-3'):
            category_title = section.find('h4')
            if not category_title:
                continue
                
            for link in section.find_all('a'):
                href = link.get('href')
                if href:
                    categories.append({
                        'title': category_title.text.strip(),
                        'name': link.text.strip(),
                        'url': urljoin(base_url, href)
                    })
                    
        print(f"Found {len(categories)} categories")
        return categories
        
    except Exception as e:
        print(f"Error getting categories: {e}")
        return categories

def get_pagination_urls(soup: BeautifulSoup, base_url: str) -> List[str]:
    """Extract pagination URLs from category page"""
    urls = []
    try:
        pagination = soup.find('ul', class_='pagination')
        if not pagination:
            return [base_url]
            
        for link in pagination.find_all('a'):
            href = link.get('href')
            if href and href != '#':
                urls.append(urljoin(base_url, href))
                
        return list(set(urls))
        
    except Exception as e:
        print(f"Error getting pagination: {e}")
        return [base_url]

def get_listing_urls(soup: BeautifulSoup, base_url: str) -> List[str]:
    """Extract listing URLs from category page"""
    urls = []
    try:
        listings = soup.find_all('div', class_='cs_card_col')
        for listing in listings:
            link = listing.find('a', class_='cs_card_img')
            if link and link.get('href'):
                urls.append(urljoin(base_url, link['href']))
                
        return urls
        
    except Exception as e:
        print(f"Error getting listing URLs: {e}")
        return []

def process_listing(session: requests.Session, url: str, stats: ScraperStats) -> Optional[Dict]:
    """Process a single listing page"""
    try:
        soup = make_request(session, url)
        if not soup:
            return None
            
        title = soup.find('h1')
        title_text = title.text.strip() if title else ''
        
        user_name = None
        phone = None
        price = None
        description = ''
        
        info_table = soup.find('table', class_='table')
        if info_table:
            for row in info_table.find_all('tr'):
                label_cell = row.find('td', width='35%')
                if not label_cell:
                    continue
                    
                label = label_cell.find('strong').text.strip() if label_cell.find('strong') else ''
                value_cell = label_cell.find_next_sibling('td')
                if not value_cell:
                    continue
                
                if 'İstifadəçi' in label:
                    user_name = value_cell.text.strip()
                elif 'Mobil' in label:
                    phone_link = value_cell.find('a')
                    if phone_link:
                        phone = phone_link.text.strip()
                elif 'Qiymət' in label:
                    price = value_cell.text.strip()
                        
        desc_elem = soup.find('p')
        if desc_elem:
            description = desc_elem.text.strip()
            
        formatted_phone = format_phone(phone, stats, phone)
        if formatted_phone:
            stats.valid_numbers += 1
            return {
                'name': user_name,
                'phone': formatted_phone,
                'website': 'birja.com',
                'link': url,
                'raw_data': {
                    'title': title_text,
                    'price': price,
                    'description': description
                }
            }
        else:
            stats.invalid_numbers += 1
            return None
            
    except Exception as e:
        print(f"Error processing listing {url}: {e}")
        return None

def scrape_category(session: requests.Session, category: Dict, stats: ScraperStats) -> List[Dict]:
    """Scrape all listings in a category"""
    items = []
    try:
        soup = make_request(session, category['url'])
        if not soup:
            return items
            
        pagination_urls = get_pagination_urls(soup, category['url'])
        print(f"Found {len(pagination_urls)} pages in category {category['name']}")
        
        for page_url in pagination_urls:
            try:
                page_soup = make_request(session, page_url)
                if not page_soup:
                    continue
                    
                listing_urls = get_listing_urls(page_soup, page_url)
                print(f"Found {len(listing_urls)} listings on page {page_url}")
                
                for listing_url in listing_urls:
                    try:
                        item = process_listing(session, listing_url, stats)
                        if item:
                            items.append(item)
                            stats.total_listings += 1
                            
                    except Exception as e:
                        print(f"Error processing listing {listing_url}: {e}")
                        continue
                    
            except Exception as e:
                print(f"Error processing page {page_url}: {e}")
                continue
                
        return items
        
    except Exception as e:
        print(f"Error scraping category {category['name']}: {e}")
        return items

def scrape() -> List[Dict]:
    """Main scraping function"""
    session = requests.Session()
    stats = ScraperStats()
    all_items = []
    
    try:
        categories = get_categories(session)
        stats.total_categories = len(categories)
        print(f"Starting scrape of {len(categories)} categories")
        
        for category in categories:
            try:
                print(f"\nProcessing category: {category['name']}")
                items = scrape_category(session, category, stats)
                if items:
                    all_items.extend(items)
                    
            except Exception as e:
                print(f"Error processing category {category['name']}: {e}")
                continue
                
        stats.print_stats()
        
    except Exception as e:
        print(f"Scraping error: {e}")
        raise
        
    return all_items