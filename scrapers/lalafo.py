import requests
import json
import time
import pandas as pd

def scrape_lalafo(category_id=2029, max_pages=2):
    """
    Scrape Lalafo API for the specified number of pages
    and save all data without filtering.
    
    Args:
        category_id: Category ID to scrape (default: 2029 for real estate)
        max_pages: Number of pages to scrape (default: 2)
    """
    # Base URL for the API
    base_url = "https://lalafo.az/api/search/v3/feed/search"
    
    # Headers based on the provided example
    headers = {
        "authority": "lalafo.az",
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8,ru;q=0.7,az;q=0.6",
        "country-id": "13",
        "device": "pc",
        "language": "az_AZ",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "sec-ch-ua": "\"Chromium\";v=\"134\", \"Not:A-Brand\";v=\"24\", \"Google Chrome\";v=\"134\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"macOS\""
    }
    
    # List to store all raw data
    all_data = []
    
    # Scrape the specified number of pages
    for page in range(1, max_pages + 1):
        print(f"Scraping page {page}...")
        
        # Set up the parameters for this page
        params = {
            "category_id": category_id,
            "expand": "url",
            "page": page,
            "per-page": 20,
            "with_feed_banner": "true"
        }
        
        # Make the request
        response = requests.get(base_url, headers=headers, params=params)
        
        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()
            
            # Store the entire response data
            all_data.append(data)
            
            # Save each page's raw data to a JSON file
            with open(f"lalafo_page_{page}.json", "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            print(f"Successfully saved page {page} data")
            
            # Extract just the listings for CSV export
            items = data.get("items", [])
            
            # Save listings for this page to CSV
            if items:
                # Create a flat structure for the items
                flat_items = []
                for item in items:
                    # Handle nested structures
                    flat_item = {}
                    
                    # Add top-level fields
                    for key, value in item.items():
                        if key != "user" and key != "images" and key != "params" and key != "tracking_info":
                            flat_item[key] = value
                    
                    # Add user fields
                    if "user" in item and item["user"]:
                        user = item["user"]
                        for user_key, user_value in user.items():
                            flat_item[f"user_{user_key}"] = user_value
                    
                    # Add parameters
                    if "params" in item and item["params"]:
                        for param in item["params"]:
                            param_name = param.get("name", "")
                            param_value = param.get("value", "")
                            flat_item[f"param_{param_name}"] = param_value
                    
                    # Add image count and first image URL
                    if "images" in item and item["images"]:
                        flat_item["image_count"] = len(item["images"])
                        flat_item["first_image_url"] = item["images"][0].get("original_url", "")
                    
                    flat_items.append(flat_item)
                
                # Create and save dataframe
                df = pd.DataFrame(flat_items)
                df.to_csv(f"lalafo_page_{page}_listings.csv", index=False)
                
                print(f"Saved {len(items)} listings from page {page} to CSV")
            
            # Be nice to the server
            if page < max_pages:
                time.sleep(2)
        else:
            print(f"Failed to fetch page {page}. Status code: {response.status_code}")
            print(response.text)
            break
    
    # Save combined data from all pages
    if all_data:
        # Combine all items from all pages
        combined_items = []
        for page_data in all_data:
            items = page_data.get("items", [])
            combined_items.extend(items)
        
        # Save complete raw JSON
        with open("lalafo_all_pages.json", "w", encoding="utf-8") as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)
        
        print(f"Saved all {len(all_data)} pages of raw data to lalafo_all_pages.json")
        
        # Save combined items to JSON
        with open("lalafo_all_items.json", "w", encoding="utf-8") as f:
            json.dump(combined_items, f, ensure_ascii=False, indent=2)
        
        print(f"Saved all {len(combined_items)} items to lalafo_all_items.json")
        
        return all_data
    
    return None

if __name__ == "__main__":
    # Scrape 2 pages from the real estate category (2029)
    data = scrape_lalafo(category_id=2029, max_pages=2)
    
    if data:
        print("\nScraping completed successfully!")
        print(f"Total pages scraped: {len(data)}")
        
        # Count total items
        total_items = sum(len(page_data.get("items", [])) for page_data in data)
        print(f"Total listings scraped: {total_items}")
        
        print("\nOutput files:")
        print("- lalafo_page_1.json, lalafo_page_2.json: Raw API responses for each page")
        print("- lalafo_page_1_listings.csv, lalafo_page_2_listings.csv: Listings from each page")
        print("- lalafo_all_pages.json: Combined raw data from all pages")
        print("- lalafo_all_items.json: All listings combined")
    else:
        print("Scraping failed.")