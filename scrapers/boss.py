# scrapers/boss.py
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import time
import random
import re
from typing import Dict, List, Optional
from dataclasses import dataclass

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
        'Referer': 'https://boss.az'
    }

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

def extract_listing_details(soup: BeautifulSoup, url: str) -> Optional[Dict]:
    """Extract details from a vacancy listing page"""
    try:
        details = {}
        
        # Extract title
        title = soup.find('h1', class_='post-title')
        if title:
            details['title'] = title.text.strip()
        
        # Extract salary
        salary = soup.find('span', class_='post-salary')
        if salary:
            details['salary'] = salary.text.strip()
            
        # Extract company name
        company = soup.find('a', class_='post-company')
        if company:
            details['company'] = company.text.strip()
        
        # Extract contact info
        contact_name = None
        phone = None
        email = None
        
        # Find contact name
        contact_params = soup.find_all('div', class_='params-i')
        for param in contact_params:
            label = param.find('div', class_='params-i-label')
            value = param.find('div', class_='params-i-val')
            if label and value and 'Əlaqədar şəxs' in label.text.strip():
                contact_name = value.text.strip()
                break
        
        # Find phone and email in params_contacts
        contacts_list = soup.find('ul', class_='params params_contacts')
        if contacts_list:
            contacts_items = contacts_list.find_all('li', class_='params-i')
            for item in contacts_items:
                label = item.find('div', class_='params-i-label')
                value = item.find('div', class_='params-i-val')
                
                if label and value:
                    label_text = label.text.strip()
                    if 'Telefon' in label_text:
                        phone_elem = value.find('a', class_='phone')
                        if phone_elem:
                            phone = phone_elem.text.strip()
                    elif 'E-mail' in label_text:
                        email_elem = value.find('a')
                        if email_elem:
                            email = email_elem.get('href', '').replace('mailto:', '')
        
        # Extract job description and requirements
        description = ''
        requirements = ''
        
        desc_elem = soup.find('dd', class_='job_description')
        if desc_elem:
            description = desc_elem.text.strip()
            
        req_elem = soup.find('dd', class_='requirements')
        if req_elem:
            requirements = req_elem.text.strip()
        
        # Format phone number
        formatted_phone = None
        if phone:
            formatted_phone = format_phone(phone, None, phone)
            
        if not formatted_phone:
            return None
            
        return {
            'name': contact_name,
            'phone': formatted_phone,
            'website': 'boss.az',
            'link': url,
            'raw_data': {
                'title': details.get('title'),
                'company': details.get('company'),
                'salary': details.get('salary'),
                'email': email,
                'description': description,
                'requirements': requirements
            }
        }
            
    except Exception as e:
        print(f"Error extracting listing details: {e}")
        return None

def get_listing_links(soup: BeautifulSoup) -> List[str]:
    """Extract all listing links from a page"""
    links = []
    listings = soup.find_all('div', class_='results-i')
    
    for listing in listings:
        link_elem = listing.find('a', class_='results-i-link')
        if link_elem and link_elem.get('href'):
            link = link_elem['href']
            if not link.startswith('http'):
                link = f"https://boss.az{link}"
            links.append(link)
    
    return links

def scrape() -> List[Dict]:
    """Main scraping function"""
    session = requests.Session()
    base_url = "https://boss.az/vacancies"
    items_to_process = []
    stats = ScraperStats()
    
    try:
        # Manual pagination control
        pages_to_scrape = [1, 2, 3]  # Explicitly define which pages to scrape
        stats.total_pages = len(pages_to_scrape)
        print(f"Will scrape {len(pages_to_scrape)} pages")
        
        # Process specified pages
        for page in pages_to_scrape:
            try:
                url = f"{base_url}?page={page}"
                print(f"\nProcessing page {page}/{len(pages_to_scrape)}")
                
                soup = make_request(session, url)
                if not soup:
                    print(f"Failed to get response for page {page}")
                    continue
                
                listing_links = get_listing_links(soup)
                print(f"Found {len(listing_links)} listings on page {page}")
                
                # Process each listing
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