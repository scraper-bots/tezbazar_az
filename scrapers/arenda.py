# scrapers/arenda.py
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import time
import random
import re
import concurrent.futures

def format_phone(phone):
    """Format phone number to standard format"""
    digits = re.sub(r'\D', '', phone)
    
    if digits.startswith('994'):
        digits = digits[3:]
    if digits.startswith('0'):
        digits = digits[1:]
    
    # Validate phone number
    if len(digits) != 9:  # Azerbaijan numbers should be 9 digits
        return None
    if not digits.startswith(('10','12', '50', '51', '55','60', '70', '77','99')):  
        return None
        
    return digits

def get_headers():
    """Returns headers that mimic a real browser"""
    return {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive'
    }

def make_request(session, url, max_retries=3):
    """Make request with retries"""
    for attempt in range(max_retries):
        try:
            response = session.get(url, headers=get_headers(), timeout=10)
            if response.status_code == 200:
                return response
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(2)
    return None

def get_total_pages(soup):
    """Extract total number of pages"""
    pagination = soup.find('div', class_='pagination_box')
    if pagination:
        pages = pagination.find_all('a', class_='page-numbers')
        if pages:
            last_page = pages[-1].text.strip()
            try:
                return int(last_page)
            except ValueError:
                pass
    return 1

def get_listing_details(session, url):
    """Get details from individual listing page"""
    try:
        response = make_request(session, url)
        if not response:
            return None

        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Get agent/owner name
        owner_elem = soup.find('div', class_='new_elan_user_info')
        owner_name = None
        if owner_elem and owner_elem.find('p'):
            owner_name = owner_elem.find('p').text.strip().split('(')[0].strip()
        
        # Get phone
        phone_elem = soup.find('p', class_='elan_in_tel_box')
        phone = None
        if phone_elem:
            phone_link = phone_elem.find('a', class_='elan_in_tel')
            if phone_link:
                phone = format_phone(phone_link.text.strip())
        
        # Get description
        description = ''
        desc_elem = soup.find('div', class_='elan_info_txt')
        if desc_elem:
            description = desc_elem.text.strip()
        
        # Get property details
        property_details = []
        property_list = soup.find('ul', class_='property_lists')
        if property_list:
            for item in property_list.find_all('li'):
                property_details.append(item.text.strip())
        
        # Get address
        address = ''
        address_elem = soup.find('span', class_='elan_unvan_txt')
        if address_elem:
            address = address_elem.text.strip()
            
        # Get price
        price = None
        price_elem = soup.find('div', class_='elan_new_price_box')
        if price_elem:
            price = price_elem.text.strip()
        
        return {
            'owner_name': owner_name,
            'phone': phone,
            'description': description,
            'property_details': property_details,
            'address': address,
            'price': price
        }
            
    except Exception as e:
        print(f"Error getting listing details: {e}")
    return None

def scrape_page(url, session):
    """Scrape single page of listings"""
    listings = []
    try:
        response = make_request(session, url)
        if not response:
            return listings

        soup = BeautifulSoup(response.text, 'html.parser')
        listing_elements = soup.find_all('li', class_='new_elan_box')
        
        for listing in listing_elements:
            try:
                link_elem = listing.find('a')
                if not link_elem:
                    continue

                link = link_elem['href']
                title = link_elem['title']
                full_url = link if link.startswith('http') else f"https://arenda.az{link}"

                # Get listing details
                details = get_listing_details(session, full_url)
                
                if details and details['phone']:  # Only save if we have a valid phone number
                    data = {
                        'name': details['owner_name'],
                        'phone': details['phone'],
                        'website': 'arenda.az',
                        'link': full_url,
                        'raw_data': {
                            'title': title,
                            'description': details['description'],
                            'property_details': details['property_details'],
                            'address': details['address'],
                            'price': details['price']
                        }
                    }
                    listings.append(data)
                    print(f"Added listing with phone: {details['phone']}")
                
            except Exception as e:
                print(f"Error processing listing: {e}")
                continue

    except Exception as e:
        print(f"Error scraping page {url}: {e}")
    
    return listings

def scrape():
    """Main scraping function"""
    session = requests.Session()
    all_listings = []
    processed_phones = set()  # Track unique phone numbers
    
    # Get total pages
    try:
        response = make_request(session, "https://arenda.az")
        if response:
            soup = BeautifulSoup(response.text, 'html.parser')
            total_pages = get_total_pages(soup)
            print(f"Found {total_pages} pages to scrape")
            
            # Create URLs for all pages
            urls = [
                f"https://arenda.az/filtirli-axtaris/{page}/?home_search=1&lang=1&site=1"
                for page in range(1, total_pages + 1)
            ]
            
            # Scrape pages in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                future_to_url = {executor.submit(scrape_page, url, session): url for url in urls}
                for future in concurrent.futures.as_completed(future_to_url):
                    try:
                        listings = future.result()
                        all_listings.extend(listings)
                    except Exception as e:
                        print(f"Error processing page results: {e}")
                        
    except Exception as e:
        print(f"Error in main scraping process: {e}")
    
    print(f"\nScraping completed. Found {len(all_listings)} listings with phone numbers")
    return all_listings

def save_to_db(db_connection, data):
    """Save scraped data to database"""
    cursor = db_connection.cursor()
    new_count = 0
    update_count = 0
    processed_phones = set()  # Track unique phone numbers
    
    # First, get existing phone numbers
    cursor.execute("SELECT phone FROM leads")
    existing_phones = {row[0] for row in cursor.fetchall()}
    
    for item in data:
        phone = item.get('phone')
        
        # Skip if we've already processed this phone in current batch
        if phone in processed_phones:
            continue
            
        processed_phones.add(phone)
        
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
            item.get('name'),
            phone,
            item.get('website'),
            item.get('link'),
            datetime.now(),
            json.dumps(item.get('raw_data'), ensure_ascii=False)
        )
        
        try:
            cursor.execute(query, values)
            result = cursor.fetchone()
            db_connection.commit()
            
            if result[0]:  # True if inserted, False if updated
                new_count += 1
                print(f"Added new phone: {phone}")
            else:
                update_count += 1
                print(f"Updated existing phone: {phone}")
                
        except Exception as e:
            print(f"Error saving data for phone {phone}: {e}")
            db_connection.rollback()
    
    print(f"\nDatabase update summary:")
    print(f"New records added: {new_count}")
    print(f"Records updated: {update_count}")
    print(f"Total unique phones processed: {len(processed_phones)}")
    cursor.close()