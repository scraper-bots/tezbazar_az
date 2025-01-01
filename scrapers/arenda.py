# scrapers/arenda.py
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

# Load environment variables
load_dotenv()

def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )

@dataclass
class PageStats:
    page_number: int
    total_links: int = 0
    total_numbers: int = 0
    valid_numbers: int = 0
    invalid_numbers: int = 0
    unique_numbers: int = 0
    db_inserts: int = 0
    db_updates: int = 0
    invalid_phone_list: List[str] = None

    def __post_init__(self):
        self.invalid_phone_list = []

class DatabaseInserter:
    def __init__(self, db_connection):
        self.db_connection = db_connection
        self.queue = queue.Queue()
        self._stop = False
        self.worker = threading.Thread(target=self._process_queue)
        self.stats = {}
        self.worker.start()

    def add_item(self, item, page_number):
        if page_number not in self.stats:
            self.stats[page_number] = PageStats(page_number)
        self.queue.put((item, page_number))

    def stop(self):
        self._stop = True
        self.worker.join()

    def _process_queue(self):
        cursor = self.db_connection.cursor()
        while not self._stop or not self.queue.empty():
            try:
                item, page_number = self.queue.get(timeout=1)
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
                    item['name'], item['phone'], item['website'],
                    item['link'], datetime.now(),
                    json.dumps(item['raw_data'], ensure_ascii=False)
                )
                
                cursor.execute(query, values)
                is_insert = cursor.fetchone()[0]
                self.db_connection.commit()
                
                if is_insert:
                    self.stats[page_number].db_inserts += 1
                    print(f"New number inserted: {item['phone']}")
                else:
                    self.stats[page_number].db_updates += 1
                    print(f"Number updated: {item['phone']}")
                
            except queue.Empty:
                continue
            except Exception as e:
                print(f"DB Error: {e}")
                self.db_connection.rollback()
            finally:
                if not self.queue.empty():
                    self.queue.task_done()
        cursor.close()

def format_phone(phone, stats=None, original=None):
    """Format and validate phone number"""
    digits = re.sub(r'\D', '', phone)
    if digits.startswith('994'): digits = digits[3:]
    if digits.startswith('0'): digits = digits[1:]
    
    if len(digits) != 9:
        if stats:
            stats.invalid_phone_list.append(f"Length error - Original: {original}, Cleaned: {digits}")
        return None
        
    if not digits.startswith(('10','12','50','51','55','60','70','77','99')):
        if stats:
            stats.invalid_phone_list.append(f"Prefix error - Original: {original}, Cleaned: {digits}")
        return None
    
    return digits

def get_headers():
    """Get randomized headers"""
    user_agents = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ]
    return {
        'User-Agent': random.choice(user_agents),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive'
    }

def make_request(session, url, max_retries=3):
    """Make HTTP request with retries and random delays"""
    for attempt in range(max_retries):
        try:
            time.sleep(random.uniform(0.5, 1.5))  # Random delay between requests
            response = session.get(url, headers=get_headers(), timeout=10)
            if response.status_code == 200: 
                return response
            print(f"Got status code {response.status_code} for {url}")
        except Exception as e:
            print(f"Request error (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if attempt == max_retries - 1: raise
            time.sleep(random.uniform(2, 4))  # Longer delay after error
    return None

def get_total_pages(soup):
    """Extract total number of pages"""
    try:
        pagination = soup.find('div', class_='pagination_box')
        if not pagination:
            print("No pagination box found")
            return 1

        # Find all page numbers including the last page
        page_numbers = []
        for link in pagination.find_all('a', class_='page-numbers'):
            try:
                num = int(link.text.strip())
                page_numbers.append(num)
            except ValueError:
                continue

        if not page_numbers:
            print("No valid page numbers found")
            print(pagination.prettify())
            return 1

        last_page = max(page_numbers)
        print(f"Found last page number: {last_page}")
        return last_page

    except Exception as e:
        print(f"Pagination error: {str(e)}")
        if pagination:
            print(pagination.prettify())
        return 1

def get_listing_details(session, url, stats):
    """Extract details from listing page"""
    try:
        response = make_request(session, url)
        if not response: return None
        soup = BeautifulSoup(response.text, 'html.parser')
        
        stats.total_numbers += 1
        phone = None
        original_phone = None
        
        phone_elem = soup.find('p', class_='elan_in_tel_box')
        if phone_elem and phone_elem.find('a', class_='elan_in_tel'):
            original_phone = phone_elem.find('a', class_='elan_in_tel').text.strip()
            phone = format_phone(original_phone, stats, original_phone)
            if phone:
                stats.valid_numbers += 1
            else:
                stats.invalid_numbers += 1

        owner_elem = soup.find('div', class_='new_elan_user_info')
        owner_name = None
        if owner_elem and owner_elem.find('p'):
            owner_name = owner_elem.find('p').text.strip().split('(')[0].strip()

        desc_elem = soup.find('div', class_='elan_info_txt')
        description = desc_elem.text.strip() if desc_elem else ''

        property_list = soup.find('ul', class_='property_lists')
        property_details = [item.text.strip() for item in property_list.find_all('li')] if property_list else []

        addr_elem = soup.find('span', class_='elan_unvan_txt')
        address = addr_elem.text.strip() if addr_elem else ''

        price_elem = soup.find('div', class_='elan_new_price_box')
        price = price_elem.text.strip() if price_elem else None

        return {
            'owner_name': owner_name,
            'phone': phone,
            'description': description,
            'property_details': property_details,
            'address': address,
            'price': price
        }
            
    except Exception as e:
        print(f"Error getting listing details from {url}: {str(e)}")
        return None

def scrape_page(url, session, db_inserter, page_number):
    """Scrape a single page of listings"""
    try:
        if page_number not in db_inserter.stats:
            db_inserter.stats[page_number] = PageStats(page_number)
            
        response = make_request(session, url)
        if not response: 
            print(f"No response from page {page_number}")
            return

        soup = BeautifulSoup(response.text, 'html.parser')
        listings = soup.find_all('li', class_='new_elan_box')
        db_inserter.stats[page_number].total_links = len(listings)
        
        print(f"\nProcessing page {page_number}")
        print(f"Found {len(listings)} listings")
        
        for idx, listing in enumerate(listings, 1):
            try:
                link_elem = listing.find('a')
                if not link_elem: 
                    print(f"No link found for listing {idx}")
                    continue
                
                full_url = (link_elem['href'] if link_elem['href'].startswith('http') 
                           else f"https://arenda.az{link_elem['href']}")
                
                print(f"Processing listing {idx}/{len(listings)}: {full_url}")
                
                details = get_listing_details(session, full_url, db_inserter.stats[page_number])
                if not details or not details['phone']:
                    print(f"No valid details/phone for listing {idx}")
                    continue
                
                db_inserter.stats[page_number].unique_numbers += 1
                item = {
                    'name': details['owner_name'],
                    'phone': details['phone'],
                    'website': 'arenda.az',
                    'link': full_url,
                    'raw_data': {
                        'title': link_elem['title'],
                        'description': details['description'],
                        'property_details': details['property_details'],
                        'address': details['address'],
                        'price': details['price']
                    }
                }
                db_inserter.add_item(item, page_number)
                print(f"Successfully processed listing {idx} with phone {details['phone']}")
                
            except Exception as e:
                print(f"Error processing listing {idx}: {str(e)}")
                continue

        stats = db_inserter.stats[page_number]
        print(f"\nPage {page_number} Stats:")
        print(f"Total links found: {stats.total_links}")
        print(f"Numbers found: {stats.total_numbers}")
        print(f"Valid numbers: {stats.valid_numbers}")
        print(f"Invalid numbers: {stats.invalid_numbers}")
        if stats.invalid_numbers > 0:
            print("Invalid phone numbers:")
            for phone in stats.invalid_phone_list:
                print(f"  {phone}")
        print(f"Unique numbers: {stats.unique_numbers}")
        print(f"DB inserts: {stats.db_inserts}")
        print(f"DB updates: {stats.db_updates}")
        
    except Exception as e:
        print(f"Error processing page {page_number}: {str(e)}")
        print(f"URL: {url}")

def scrape():
    """Main scraping function"""
    session = requests.Session()
    session.headers.update(get_headers())
    base_url = "https://arenda.az/filtirli-axtaris"
    search_params = "?home_search=1&lang=1&site=1&home_s=1&price_min=&price_max=&axtar=&sahe_min=&sahe_max=&mertebe_min=&mertebe_max=&y_mertebe_min=&y_mertebe_max="
    
    conn = None
    db_inserter = None
    
    try:
        # Manually defined pages
        pages_to_scrape = [1, 2, 3]
        print(f"Scraping pages: {pages_to_scrape}")
        
        urls = []
        for page in pages_to_scrape:
            if page == 1:
                urls.append(f"{base_url}/{search_params}")
            else:
                urls.append(f"{base_url}/{page}/{search_params}")
        
        conn = get_db_connection()
        db_inserter = DatabaseInserter(conn)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(scrape_page, url, session, db_inserter, page_num)
                for page_num, url in enumerate(urls, 1)
            ]
            concurrent.futures.wait(futures)
            
        # Print final statistics
        total_stats = PageStats(0)
        for page_stats in db_inserter.stats.values():
            total_stats.total_links += page_stats.total_links
            total_stats.total_numbers += page_stats.total_numbers
            total_stats.valid_numbers += page_stats.valid_numbers
            total_stats.invalid_numbers += page_stats.invalid_numbers
            total_stats.unique_numbers += page_stats.unique_numbers
            total_stats.db_inserts += page_stats.db_inserts
            total_stats.db_updates += page_stats.db_updates
            
        print("\nFinal Statistics:")
        print(f"Total pages processed: {len(db_inserter.stats)}/{len(pages_to_scrape)}")
        print(f"Total links found: {total_stats.total_links}")
        print(f"Total numbers found: {total_stats.total_numbers}")
        print(f"Valid numbers: {total_stats.valid_numbers}")
        print(f"Invalid numbers: {total_stats.invalid_numbers}")
        print(f"Unique numbers: {total_stats.unique_numbers}")
        print(f"New records inserted: {total_stats.db_inserts}")
        print(f"Records updated: {total_stats.db_updates}")
        
    except Exception as e:
        print(f"Scrape error: {e}")
        raise  # Re-raise the exception for the main script to handle

    finally:
        if db_inserter:
            db_inserter.stop()
        if conn:
            conn.close()
            
    return []