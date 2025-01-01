# scrapers/autonet.py
import requests
from datetime import datetime
import json
import time
import random
import re
from typing import Dict, List, Optional, Tuple
import psycopg2
import os
from dotenv import load_dotenv
from dataclasses import dataclass

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
class ScraperStats:
    total_items: int = 0
    valid_numbers: int = 0
    invalid_numbers: int = 0
    db_inserts: int = 0
    db_updates: int = 0
    multi_phone_items: int = 0
    invalid_phone_list: List[str] = None

    def __post_init__(self):
        self.invalid_phone_list = []

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

def get_headers() -> Dict[str, str]:
    """Get request headers for autonet.az API"""
    user_agents = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
    ]
    
    return {
        'Accept': 'application/json',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8,ru;q=0.7,az;q=0.6',
        'Connection': 'keep-alive',
        'User-Agent': random.choice(user_agents),
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': 'https://autonet.az/items',
        'Origin': 'https://autonet.az',
        'DNT': '1',
        'Host': 'autonet.az',
        'Authorization': 'Bearer null',
        'X-Authorization': '00028c2ddcc1ca6c32bc919dca64c288bf32ff2a',
        'X-XSRF-TOKEN': 'eyJpdiI6Im0wUnJnSkx4VUk3MGZ0U1FjV01aaFE9PSIsInZhbHVlIjoiZkkyenVqaFZPQVwvd09ZUk9YKzNCMUtreXpuZm5GNFdKd0FEMUljaUNDVHpVMWd2TDJJbG9UMWFEaHFxaGdhV1kiLCJtYWMiOiI5NWQzZmM1ZTNhOTZhMjQ2Y2Q1MzRjOThkMmM5YmNlMGM2NGRjOTNiMzY2OTUyMmU1ODM3MjcxNzdiYTY4YzA3In0=',
        'Cookie': '*ga=GA1.1.1222610526.1735019110; *fbp=fb.1.1735019112029.539904306565835293; XSRF-TOKEN=eyJpdiI6Im0wUnJnSkx4VUk3MGZ0U1FjV01aaFE9PSIsInZhbHVlIjoiZkkyenVqaFZPQVwvd09ZUk9YKzNCMUtreXpuZm5GNFdKd0FEMUljaUNDVHpVMWd2TDJJbG9UMWFEaHFxaGdhV1kiLCJtYWMiOiI5NWQzZmM1ZTNhOTZhMjQ2Y2Q1MzRjOThkMmM5YmNlMGM2NGRjOTNiMzY2OTUyMmU1ODM3MjcxNzdiYTY4YzA3In0%3D; autonet_session=eyJpdiI6ImJSV3AyRFcyY1ZXelUwcjVHTko2RHc9PSIsInZhbHVlIjoibFp1amlyRXlZNnVmdHg2RDdYVkx2QlhadzJtdVloT3c4b2NvUHNoVG9KR2xwRDhlYkhHM0dmMDNmYkNZSzk1SSIsIm1hYyI6IjNlNGNlNDM3N2JjMGRjODg2ODAzZWY4MThmZjI2ZGQ3YWIxMjM2OWY4NTE1OTQ4Nzg4NDAwYjc4ZTg2MTZhZTIifQ%3D%3D; *ga*9BNXHJFLEV=GS1.1.1735723348.12.0.1735723371.37.0.0',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin'
    }

def make_request(session: requests.Session, url: str, max_retries: int = 3) -> Optional[Dict]:
    """Make API request with retries and random delays"""
    for attempt in range(max_retries):
        try:
            time.sleep(random.uniform(1, 2))  # Random delay between requests
            response = session.get(url, headers=get_headers(), timeout=10)
            
            if response.status_code == 200:
                return response.json()
            
            print(f"Got status code {response.status_code} for {url}")
            
        except Exception as e:
            print(f"Request error (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if attempt == max_retries - 1:
                raise
            time.sleep(random.uniform(2, 4))  # Longer delay after error
    
    return None

def insert_lead(cursor, item: Dict, phone: str, stats: ScraperStats) -> None:
    """Insert or update a single lead in the database"""
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
            item.get('fullname'),
            phone,
            'autonet.az',
            f"https://autonet.az/items/{item.get('id')}",
            datetime.now(),
            json.dumps(item, ensure_ascii=False)
        )
        
        cursor.execute(query, values)
        is_insert = cursor.fetchone()[0]
        
        if is_insert:
            stats.db_inserts += 1
            print(f"New number inserted: {phone}")
        else:
            stats.db_updates += 1
            print(f"Number updated: {phone}")
            
        return True
    
    except Exception as e:
        print(f"Error inserting/updating lead: {e}")
        return False

def save_to_db(conn, items: List[Dict]) -> None:
    """Save scraped items to database, handling multiple phone numbers per item"""
    stats = ScraperStats()
    cursor = conn.cursor()
    stats.total_items = len(items)
    
    for item in items:
        try:
            # Format and validate both phone numbers
            phone1 = format_phone(item.get('phone1'), stats, item.get('phone1'))
            phone2 = format_phone(item.get('phone2'), stats, item.get('phone2'))
            
            # Track valid/invalid numbers
            valid_phones = []
            if phone1:
                valid_phones.append(phone1)
                stats.valid_numbers += 1
            else:
                stats.invalid_numbers += 1
                
            if phone2:
                valid_phones.append(phone2)
                stats.valid_numbers += 1
            else:
                stats.invalid_numbers += 1
            
            # Skip if no valid phone numbers
            if not valid_phones:
                continue
            
            # If we have multiple valid phone numbers, track it
            if len(valid_phones) > 1:
                stats.multi_phone_items += 1
            
            # Insert each valid phone number as a separate row
            for phone in valid_phones:
                success = insert_lead(cursor, item, phone, stats)
                if success:
                    conn.commit()
                else:
                    conn.rollback()
                
        except Exception as e:
            print(f"Error processing item: {e}")
            conn.rollback()
            continue
            
    cursor.close()
    
    # Print statistics
    print("\nScraping Statistics:")
    print(f"Total items processed: {stats.total_items}")
    print(f"Total valid numbers: {stats.valid_numbers}")
    print(f"Total invalid numbers: {stats.invalid_numbers}")
    print(f"Items with multiple phones: {stats.multi_phone_items}")
    print(f"New records inserted: {stats.db_inserts}")
    print(f"Records updated: {stats.db_updates}")
    if stats.invalid_numbers > 0:
        print("\nInvalid phone numbers:")
        for phone in stats.invalid_phone_list:
            print(f"  {phone}")

def scrape() -> List[Dict]:
    """Main scraping function"""
    session = requests.Session()
    base_url = "https://autonet.az/api/items/searchItem"
    items_to_process = []
    
    try:
        # Get first page to determine total pages
        response = make_request(session, base_url)
        if not response:
            print("Failed to get initial response")
            return []
            
        total_pages = response.get('last_page', 1)
        print(f"Found {total_pages} pages to scrape")
        
        # Manual pagination control
        pages_to_scrape = [1, 2, 3]  # Explicitly define which pages to scrape
        print(f"Will scrape pages: {pages_to_scrape}")
        
        # Process specified pages
        for page in pages_to_scrape:
            try:
                url = f"{base_url}?page={page}"
                print(f"\nProcessing page {page}/{pages_to_scrape}")
                
                response = make_request(session, url)
                if not response:
                    print(f"Failed to get response for page {page}")
                    continue
                
                page_items = response.get('data', [])
                print(f"Found {len(page_items)} items on page {page}")
                
                items_to_process.extend(page_items)
                
            except Exception as e:
                print(f"Error processing page {page}: {e}")
                continue
        
        # Save all items to database
        if items_to_process:
            conn = get_db_connection()
            try:
                save_to_db(conn, items_to_process)
            finally:
                conn.close()
        
    except Exception as e:
        print(f"Scraping error: {e}")
    
    return items_to_process  # Return items for main script to handle