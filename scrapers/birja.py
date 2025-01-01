# scrapers/birja.py
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import time
import random
import re
import concurrent.futures
import queue
import threading
from dataclasses import dataclass
import psycopg2
import os
from dotenv import load_dotenv
from typing import Dict, List, Optional
from urllib.parse import urljoin

# Load environment variables
load_dotenv()

@dataclass
class ScraperStats:
    total_categories: int = 0
    total_listings: int = 0
    valid_numbers: int = 0
    invalid_numbers: int = 0
    db_inserts: int = 0
    db_updates: int = 0
    invalid_phone_list: List[str] = None

    def __post_init__(self):
        self.invalid_phone_list = []

def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )

def get_headers():
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

def format_phone(phone: str, stats: ScraperStats, original: str = None) -> Optional[str]:
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
    
    # Validate fourth digit (should be 2-9)
    if digits[3] in ('0', '1'):
        if stats:
            stats.invalid_phone_list.append(f"Fourth digit error - Original: {original}, Cleaned: {digits}")
        return None
        
    return digits

def make_request(session: requests.Session, url: str, max_retries: int = 3) -> Optional[BeautifulSoup]:
    """Make HTTP request with retries and random delays"""
    for attempt in range(max_retries):
        try:
            time.sleep(random.uniform(1, 2))  # Random delay between requests
            response = session.get(url, headers=get_headers(), timeout=10)
            
            if response.status_code == 200:
                return BeautifulSoup(response.text, 'html.parser')
            
            print(f"Got status code {response.status_code} for {url}")
            
        except Exception as e:
            print(f"Request error (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if attempt == max_retries - 1:
                raise
            time.sleep(random.uniform(2, 4))  # Longer delay after error
    
    return None

def get_categories(session: requests.Session) -> List[Dict]:
    """Get all category links from the main categories page"""
    base_url = "https://birja.com/all_category/az"
    categories = []
    
    try:
        soup = make_request(session, base_url)
        if not soup:
            return categories
            
        # Find all category sections
        for section in soup.find_all('div', class_='col-md-3'):
            category_title = section.find('h4')
            if not category_title:
                continue
                
            # Get all links in this category section
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
                
        return list(set(urls))  # Remove duplicates
        
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
            
        # Extract listing details
        title = soup.find('h1')
        title_text = title.text.strip() if title else ''
        
        # Get user info
        user_name = None
        phone = None
        price = None
        ad_id = None
        
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
                elif 'Elanın nömrəsi' in label:
                    ad_id = value_cell.text.strip()
                        
        # Get description
        description = ''
        desc_elem = soup.find('p')
        if desc_elem:
            description = desc_elem.text.strip()
            
        # Validate phone number
        formatted_phone = format_phone(phone, stats, phone)
        if formatted_phone:
            stats.valid_numbers += 1
        else:
            stats.invalid_numbers += 1
            return None
            
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
        
    except Exception as e:
        print(f"Error processing listing {url}: {e}")
        return None

def save_to_db(conn, items: List[Dict], stats: ScraperStats) -> None:
    """Save scraped items to database"""
    cursor = conn.cursor()
    
    for item in items:
        try:
            query = """
                INSERT INTO leads (name, phone, website, link, scraped_at, raw_data)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (phone) DO UPDATE
                SET name = EXCLUDED.name,
                    website = EXCLUDED.website,
                    link = EXCLUDED.link,
                    scraped_at = EXCLUDED.scraped_at,
                    raw_data = EXCLUDED.raw_data
                RETURNING (xmax = 0) AS inserted;
            """
            
            values = (
                item['name'],
                item['phone'],
                item['website'],
                item['link'],
                datetime.now(),
                json.dumps(item['raw_data'], ensure_ascii=False)
            )
            
            cursor.execute(query, values)
            is_insert = cursor.fetchone()[0]
            
            if is_insert:
                stats.db_inserts += 1
                print(f"New number inserted: {item['phone']}")
            else:
                stats.db_updates += 1
                print(f"Number updated: {item['phone']}")
                
            conn.commit()
            
        except Exception as e:
            print(f"Error saving item to database: {e}")
            conn.rollback()
            continue
            
    cursor.close()

def scrape_category(session: requests.Session, category: Dict, stats: ScraperStats) -> List[Dict]:
    """Scrape all listings in a category"""
    items = []
    try:
        # Get initial category page
        soup = make_request(session, category['url'])
        if not soup:
            return items
            
        # Get all pagination URLs
        pagination_urls = get_pagination_urls(soup, category['url'])
        print(f"Found {len(pagination_urls)} pages in category {category['name']}")
        
        # Process each page
        for page_url in pagination_urls:
            try:
                page_soup = make_request(session, page_url)
                if not page_soup:
                    continue
                    
                # Get listing URLs from this page
                listing_urls = get_listing_urls(page_soup, page_url)
                print(f"Found {len(listing_urls)} listings on page {page_url}")
                
                # Process each listing
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

def scrape():
    """Main scraping function"""
    session = requests.Session()
    stats = ScraperStats()
    conn = None
    
    try:
        # Get all categories
        categories = get_categories(session)
        stats.total_categories = len(categories)
        print(f"Starting scrape of {len(categories)} categories")
        
        # Process each category
        conn = get_db_connection()
        for category in categories:
            try:
                print(f"\nProcessing category: {category['name']}")
                items = scrape_category(session, category, stats)
                
                if items:
                    save_to_db(conn, items, stats)
                    
            except Exception as e:
                print(f"Error processing category {category['name']}: {e}")
                continue
                
        # Print final statistics
        print("\nScraping Statistics:")
        print(f"Total categories processed: {stats.total_categories}")
        print(f"Total listings found: {stats.total_listings}")
        print(f"Valid numbers: {stats.valid_numbers}")
        print(f"Invalid numbers: {stats.invalid_numbers}")
        print(f"New records inserted: {stats.db_inserts}")
        print(f"Records updated: {stats.db_updates}")
        if stats.invalid_numbers > 0:
            print("\nInvalid phone numbers:")
            for phone in stats.invalid_phone_list:
                print(f"  {phone}")
                
    except Exception as e:
        print(f"Scraping error: {e}")
        raise
        
    finally:
        if conn:
            conn.close()