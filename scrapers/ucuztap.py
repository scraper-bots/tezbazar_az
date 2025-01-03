# scrapers/ucuztap.py
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import time
import random
import re
import urllib3
from typing import Dict, List, Optional, Set
from dataclasses import dataclass
from urllib.parse import urljoin

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
        print("\nScraping Statistics:")
        print(f"Total pages processed: {self.total_pages}")
        print(f"Total listings found: {self.total_listings}")
        print(f"Valid numbers found: {self.valid_numbers}")
        print(f"Invalid numbers found: {self.invalid_numbers}")
        if self.invalid_numbers > 0:
            print("\nInvalid phone numbers:")
            for phone in self.invalid_phone_list:
                print(f"  {phone}")

def get_listing_links_from_sitemap(html_content: str) -> List[str]:
    """Extract listing URLs from sitemap HTML page"""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        urls = set()  # Use set to avoid duplicates

        # Look for product divs
        product_divs = soup.find_all('div', attrs={'data-id': True})
        print(f"Found {len(product_divs)} product divs")

        # Look for links in product divs
        for div in product_divs:
            link = div.find('a', href=True)
            if link and link.get('href'):
                if '/elan/' in link['href']:
                    full_url = link['href']
                    if not full_url.startswith('http'):
                        full_url = urljoin('https://ucuztap.az', full_url)
                    urls.add(full_url)
                    print(f"Found listing URL: {full_url}")
        
        # Alternative method: Look for product links directly
        for link in soup.find_all('a', href=re.compile(r'/elan/\d+')):
            full_url = link['href']
            if not full_url.startswith('http'):
                full_url = urljoin('https://ucuztap.az', full_url)
            urls.add(full_url)
            print(f"Found additional listing URL: {full_url}")

        # Debug
        if not urls:
            print("No listing URLs found. Saving HTML for debugging...")
            with open('debug_page.html', 'w', encoding='utf-8') as f:
                f.write(html_content)
            print("Debug HTML saved to debug_page.html")
        else:
            print(f"Found {len(urls)} unique listing URLs")

        return list(urls)

    except Exception as e:
        print(f"Error parsing HTML: {e}")
        print("HTML snippet:")
        print(html_content[:500] + "..." if len(html_content) > 500 else html_content)
        return []

def format_phone(phone: str, stats: Optional[ScraperStats] = None, original: str = None) -> Optional[str]:
    """Format and validate phone number with relaxed validation"""
    if not phone:
        return None
        
    # Remove all non-digit characters
    digits = re.sub(r'\D', '', phone)
    print(f"Cleaned phone digits: {digits}")
    
    # Remove country code if present
    if digits.startswith('994'): 
        digits = digits[3:]
    if digits.startswith('0'): 
        digits = digits[1:]
    
    print(f"After prefix removal: {digits}")
    
    # Validate length
    if len(digits) != 9:
        if stats:
            stats.invalid_phone_list.append(f"Length error - Original: {original}, Cleaned: {digits}")
        print(f"Invalid length ({len(digits)})")
        return None
    
    # Get the prefix (first two digits)
    prefix = digits[:2]
    
    # Validate prefix less strictly
    valid_prefixes = ('10', '12', '50', '51', '55', '60', '70', '77', '99')
    if not prefix in valid_prefixes:
        if stats:
            stats.invalid_phone_list.append(f"Prefix error - Original: {original}, Cleaned: {digits}")
        print(f"Invalid prefix ({prefix})")
        return None
    
    # Special handling for phone format patterns:
    if prefix in ('50', '51', '55', '70', '77'):
        pattern = re.compile(r'^(\d{2})([2-9])(\d{2})(\d{2})(\d{2})$')
        if pattern.match(digits):
            print(f"Valid phone number: {digits}")    
            return digits
            
    print(f"Valid phone number: {digits}")    
    return digits

def get_headers() -> Dict[str, str]:
    """Get randomized headers"""
    user_agents = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ]
    
    return {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'az,en-US;q=0.7,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'User-Agent': random.choice(user_agents),
        'Referer': 'https://ucuztap.az',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin'
    }

def make_request(session: requests.Session, url: str, max_retries: int = 3) -> Optional[BeautifulSoup]:
    """Make HTTP request with retries and error handling"""
    for attempt in range(max_retries):
        try:
            time.sleep(random.uniform(1.5, 3))
            print(f"Making request attempt {attempt + 1} for: {url}")
            
            response = session.get(
                url, 
                headers=get_headers(), 
                timeout=15,
                verify=False
            )
            
            if response.status_code == 200:
                print(f"Successfully fetched {url}")
                content_type = response.headers.get('content-type', '').lower()
                if 'text/html' not in content_type:
                    print(f"Warning: Unexpected content type: {content_type}")
                return BeautifulSoup(response.text, 'html.parser')
            
            print(f"Got status code {response.status_code} for {url}")
            
        except requests.exceptions.SSLError as e:
            print(f"SSL Error (attempt {attempt + 1}/{max_retries}): {e}")
            time.sleep(random.uniform(2, 4))
        except requests.exceptions.RequestException as e:
            print(f"Request error (attempt {attempt + 1}/{max_retries}): {e}")
            time.sleep(random.uniform(2, 4))
        except Exception as e:
            print(f"Unexpected error (attempt {attempt + 1}/{max_retries}): {e}")
            time.sleep(random.uniform(2, 4))
            
    return None

def extract_phone_number(soup: BeautifulSoup) -> Optional[str]:
    """Extract phone number from the listing page with improved extraction"""
    try:
        phone_numbers = set()
        
        # Method 1: Look for phone in fs-20 class
        strong_elements = soup.find_all('strong', class_='fs-20')
        for elem in strong_elements:
            print(f"Found strong element with fs-20 class: {elem}")
            
            phone_text = ''
            for content in elem.contents:
                if isinstance(content, str):
                    phone_text += content
                elif hasattr(content, 'name') and content.name == 'img':
                    break
            
            if phone_text:
                phone_text = phone_text.strip()
                print(f"Found phone text: {phone_text}")
                cleaned = re.sub(r'[\(\)\s\-\+]', '', phone_text)
                if cleaned:
                    phone_numbers.add(cleaned)

        # Method 2: Search in divs with specific classes
        phone_containers = soup.find_all(['div', 'span', 'p'], 
            class_=['phone', 'phones', 'contact-phone', 'phone-number'])
        for container in phone_containers:
            text = container.get_text(strip=True)
            print(f"Found phone container: {text}")
            cleaned = re.sub(r'[\(\)\s\-\+]', '', text)
            if cleaned:
                phone_numbers.add(cleaned)

        # Method 3: Look for tel: links
        tel_links = soup.find_all('a', href=re.compile(r'^tel:'))
        for link in tel_links:
            href = link.get('href', '')
            print(f"Found tel link: {href}")
            cleaned = re.sub(r'[\(\)\s\-\+:tel]', '', href)
            if cleaned:
                phone_numbers.add(cleaned)

        if phone_numbers:
            print(f"Found phone numbers: {phone_numbers}")
            return next(iter(phone_numbers))
            
        print("No phone numbers found")
        return None
        
    except Exception as e:
        print(f"Error extracting phone number: {e}")
        return None

def extract_listing_details(soup: BeautifulSoup, url: str, stats: ScraperStats) -> Optional[Dict]:
    """Extract details from a listing page"""
    try:
        print(f"\nExtracting details from {url}")
        details = {}
        
        # Extract title
        title_elem = soup.find('h1', class_='fs-24')
        if title_elem and title_elem.find('strong'):
            details['title'] = title_elem.find('strong').text.strip()
            print(f"Found title: {details['title']}")

        # Extract price
        price_elem = soup.find('button', class_='btn-price')
        if price_elem and price_elem.find('strong'):
            details['price'] = re.sub(r'\D', '', price_elem.find('strong').text.strip())
            print(f"Found price: {details['price']}")

        # Extract phone number
        raw_phone = extract_phone_number(soup)
        if not raw_phone:
            print("No phone number found")
            return None
        print(f"Raw phone: {raw_phone}")
            
        formatted_phone = format_phone(raw_phone, stats, raw_phone)
        if not formatted_phone:
            print("Invalid phone number format")
            return None
        print(f"Formatted phone: {formatted_phone}")

        # Extract other details
        seller_name = None
        shop_elem = soup.find('h3', class_='m-t-1')
        if shop_elem:
            seller_name = shop_elem.text.strip()
        
        if not seller_name:
            name_elem = soup.find('div', class_='btn-circle-120')
            if name_elem and name_elem.find('strong'):
                seller_name = name_elem.find('strong').text.strip()

        category = None
        cat_elem = soup.find('a', class_='fs-15 f-light')
        if cat_elem:
            category = cat_elem.text.strip()

        # Build final item structure
        return {
            'name': seller_name,
            'phone': formatted_phone,
            'website': 'ucuztap.az',
            'link': url,
            'raw_data': {
                'title': details.get('title'),
                'price': details.get('price'),
                'category': category
            }
        }

    except Exception as e:
        print(f"Error extracting listing details from {url}: {e}")
        return None

def scrape() -> List[Dict]:
    """Main scraping function"""
    session = requests.Session()
    session.verify = False
    
    items_to_process = []
    processed_urls = set()
    stats = ScraperStats()
    
    try:
        pages_to_scrape = [1, 2, 3]
        stats.total_pages = len(pages_to_scrape)
        print(f"Will scrape {len(pages_to_scrape)} pages")
        
        for page in pages_to_scrape:
            try:
                if page == 1:
                    url = "https://ucuztap.az/elanlar/"
                else:
                    url = f"https://ucuztap.az/elanlar/page{page}.html"
                print(f"\nProcessing page {page}/{len(pages_to_scrape)}")
                
                soup = make_request(session, url)
                if not soup:
                    print(f"Failed to get page content for {url}")
                    continue
                
                listing_urls = get_listing_links_from_sitemap(str(soup))
                new_urls = [url for url in listing_urls if url not in processed_urls]
                print(f"Found {len(new_urls)} new listings on page {page}")
                
                for idx, url in enumerate(new_urls, 1):
                    try:
                        if url in processed_urls:
                            continue
                            
                        if not url.startswith('http'):
                            url = urljoin('https://ucuztap.az', url)
                            
                        print(f"Processing listing {idx}/{len(new_urls)}: {url}")
                        
                        listing_soup = make_request(session, url)
                        if not listing_soup:
                            print(f"Failed to get listing details for {url}")
                            continue
                        
                        details = extract_listing_details(listing_soup, url, stats)
                        if details:
                            items_to_process.append(details)
                            stats.valid_numbers += 1
                            print(f"Successfully processed listing with phone {details['phone']}")
                        else:
                            stats.invalid_numbers += 1
                            print(f"No valid phone number found for listing {url}")
                            
                        stats.total_listings += 1
                        processed_urls.add(url)
                        
                        # Add a small delay between listings
                        time.sleep(random.uniform(0.5, 1.5))
                        
                    except Exception as e:
                        print(f"Error processing listing {url}: {e}")
                        continue
                    
            except Exception as e:
                print(f"Error processing page {page}: {e}")
                continue
                
            # Add a delay between pages
            time.sleep(random.uniform(2, 4))
        
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