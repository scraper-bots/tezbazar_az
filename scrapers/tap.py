import requests
from bs4 import BeautifulSoup
import json
import csv
import time
import re
import os
from urllib.parse import urljoin

class TapAzScraper:
    def __init__(self, base_url="https://tap.az", category_url="/elanlar/dasinmaz-emlak"):
        self.base_url = base_url
        self.category_url = category_url
        self.full_url = urljoin(self.base_url, self.category_url)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Referer': 'https://tap.az/'
        })
        
    def get_page(self, url):
        """Get the HTML content of a page"""
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            print(f"Error fetching page {url}: {e}")
            return None
    
    def get_phone_number(self, ad_id):
        """Get the phone number for a listing using the API endpoint"""
        phone_url = f"{self.base_url}/ads/{ad_id}/phones"
        try:
            # Add a small delay to avoid getting blocked
            time.sleep(1)
            response = self.session.post(phone_url)
            response.raise_for_status()
            data = response.json()
            return data.get('phones', [])
        except requests.RequestException as e:
            print(f"Error fetching phone number for ad {ad_id}: {e}")
            return []
        except json.JSONDecodeError:
            print(f"Error parsing phone number response for ad {ad_id}")
            return []
    
    def parse_listings(self, html):
        """Parse the HTML to extract listing information"""
        soup = BeautifulSoup(html, 'html.parser')
        listings = []
        
        # Find all product items
        product_items = soup.select('div.products-i')
        
        for item in product_items:
            try:
                # Extract the listing link
                link_element = item.select_one('a.products-link')
                if not link_element:
                    continue
                
                link = urljoin(self.base_url, link_element.get('href', ''))
                
                # Extract ad ID from the link or from data attribute
                ad_id = None
                bookmark_btn = item.select_one('button.product-bookmarks__link')
                if bookmark_btn:
                    ad_id = bookmark_btn.get('data-ad-id')
                
                # If ad_id is not found, try to extract from the URL
                if not ad_id and link:
                    ad_id_match = re.search(r'/(\d+)(?:/bookmark)?$', link)
                    if ad_id_match:
                        ad_id = ad_id_match.group(1)
                
                # Extract image URL
                img_element = item.select_one('img')
                image_url = img_element.get('src', '') if img_element else ''
                
                # Extract title/name
                name_element = item.select_one('div.products-name')
                name = name_element.text.strip() if name_element else ''
                
                # Extract price
                price_val_element = item.select_one('span.price-val')
                price_cur_element = item.select_one('span.price-cur')
                
                price_value = price_val_element.text.strip() if price_val_element else ''
                price_currency = price_cur_element.text.strip() if price_cur_element else ''
                price = f"{price_value} {price_currency}".strip()
                
                # Extract location and date
                created_element = item.select_one('div.products-created')
                location_date = created_element.text.strip() if created_element else ''
                
                # Try to split location and date
                location = ''
                date = ''
                if ',' in location_date:
                    parts = location_date.split(',', 1)
                    location = parts[0].strip()
                    date = parts[1].strip()
                
                # Add to listings
                listing = {
                    'id': ad_id,
                    'name': name,
                    'price': price,
                    'location': location,
                    'date': date,
                    'link': link,
                    'image_url': image_url
                }
                
                listings.append(listing)
                
            except Exception as e:
                print(f"Error parsing listing: {e}")
                continue
        
        # Find pagination link for next page
        next_page_url = None
        next_link = soup.select_one('div.pagination div.next a')
        if next_link:
            next_page_url = urljoin(self.base_url, next_link.get('href', ''))
        
        return listings, next_page_url
    
    def scrape_listings(self, max_pages=5):
        """Scrape listings from multiple pages"""
        all_listings = []
        current_url = self.full_url
        page = 1
        
        while current_url and page <= max_pages:
            print(f"Scraping page {page}: {current_url}")
            html = self.get_page(current_url)
            
            if not html:
                break
            
            listings, next_page_url = self.parse_listings(html)
            
            # Fetch phone numbers for each listing
            for listing in listings:
                if listing.get('id'):
                    phones = self.get_phone_number(listing['id'])
                    listing['phones'] = phones
                    # Print progress
                    print(f"Found listing: {listing['name']} - {listing['price']} - Phone: {phones}")
            
            all_listings.extend(listings)
            
            # Move to next page
            current_url = next_page_url
            page += 1
            
            # Add delay between pages
            time.sleep(2)
        
        return all_listings
    
    def save_to_csv(self, listings, filename="tap_az_listings.csv"):
        """Save the listings to a CSV file"""
        if not listings:
            print("No listings to save")
            return
        
        fieldnames = ['id', 'name', 'price', 'location', 'date', 'phones', 'link', 'image_url']
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for listing in listings:
                # Convert phones list to string
                if 'phones' in listing and isinstance(listing['phones'], list):
                    listing['phones'] = ', '.join(listing['phones'])
                
                writer.writerow(listing)
        
        print(f"Saved {len(listings)} listings to {filename}")

# Example usage
if __name__ == "__main__":
    scraper = TapAzScraper()
    listings = scraper.scrape_listings(max_pages=3)  # Limit to 3 pages for testing
    scraper.save_to_csv(listings)