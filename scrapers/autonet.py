import aiohttp
import asyncio
import json
from typing import List, Dict
import logging
import pandas as pd
from datetime import datetime
from http.cookies import SimpleCookie
import urllib.parse
from pathlib import Path
from tqdm import tqdm
import time
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AutonetScraper:
    def __init__(self, base_url: str = "https://autonet.az/api/items/searchItem"):
        self.base_url = base_url
        self.results = []
        self.cookies = {}
        self.x_auth_token = "00028c2ddcc1ca6c32bc919dca64c288bf32ff2a"
        self.semaphore = asyncio.Semaphore(10)  # Limit concurrent requests
        
        # Create data directory
        self.data_dir = Path('data')
        self.data_dir.mkdir(exist_ok=True)

    async def _get_tokens(self, session: aiohttp.ClientSession) -> None:
        """Get CSRF token and session token from main page"""
        try:
            async with session.get("https://autonet.az/items") as response:
                if response.status == 200:
                    if 'set-cookie' in response.headers:
                        cookie = SimpleCookie()
                        for cookie_str in response.headers.getall('set-cookie', []):
                            cookie.load(cookie_str)
                            for key, morsel in cookie.items():
                                self.cookies[key] = morsel.value
                                if key == 'XSRF-TOKEN':
                                    self.cookies['XSRF-TOKEN'] = urllib.parse.unquote(morsel.value)
                    
                    logger.info("Successfully obtained cookies and tokens")
                else:
                    logger.error(f"Failed to get tokens: {response.status}")
        except Exception as e:
            logger.error(f"Error getting tokens: {str(e)}")
            raise

    def _get_headers(self) -> Dict[str, str]:
        """Get headers for request"""
        xsrf_token = self.cookies.get('XSRF-TOKEN', '')
        
        return {
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
            "Host": "autonet.az",
            "Referer": "https://autonet.az/items",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "X-Authorization": self.x_auth_token,
            "X-XSRF-TOKEN": xsrf_token
        }

    async def _fetch_page(self, session: aiohttp.ClientSession, page: int, pbar: tqdm) -> Dict:
        """Fetch a single page of results with semaphore"""
        async with self.semaphore:
            try:
                params = {"page": str(page)}
                headers = self._get_headers()
                
                async with session.get(
                    self.base_url, 
                    params=params,
                    headers=headers,
                    cookies=self.cookies
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        pbar.update(1)
                        return data
                    else:
                        logger.error(f"Error fetching page {page}: {response.status}")
                        return None

            except Exception as e:
                logger.error(f"Error on page {page}: {str(e)}")
                return None

    async def _save_progress(self, current_results: List[Dict], page_num: int) -> None:
        """Save progress periodically"""
        if current_results:
            progress_filename = self.data_dir / f'autonet_progress_{page_num}.csv'
            df = pd.DataFrame(current_results)
            df.to_csv(progress_filename, index=False, encoding='utf-8')
            logger.info(f"Progress saved to {progress_filename}")

    async def scrape(self, start_page: int = 1, end_page: int = 236) -> List[Dict]:
        """Scrape all pages concurrently"""
        try:
            logger.info(f"Starting scrape from page {start_page} to {end_page}")
            
            session_timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=session_timeout) as session:
                # Get initial tokens
                await self._get_tokens(session)
                
                # Create tasks for all pages
                with tqdm(total=end_page-start_page+1, desc="Scraping pages") as pbar:
                    tasks = []
                    for page in range(start_page, end_page + 1):
                        task = self._fetch_page(session, page, pbar)
                        tasks.append(task)
                    
                    # Execute all tasks concurrently
                    results = await asyncio.gather(*tasks)
                    
                    # Process results
                    for i, data in enumerate(results, start=1):
                        if data and "data" in data:
                            self.results.extend(data["data"])
                            # Save progress every 50 pages
                            if i % 50 == 0:
                                await self._save_progress(self.results, i)

            return self.results

        except Exception as e:
            logger.error(f"Scraping failed: {str(e)}")
            raise

    def save_to_csv(self, filename: str = None) -> None:
        """Save results to CSV file in data directory"""
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = self.data_dir / f"autonet_data_{timestamp}.csv"
        else:
            filename = self.data_dir / filename
        
        df = pd.DataFrame(self.results)
        
        if not df.empty:
            # Clean numeric columns
            numeric_columns = ['price', 'id', 'engine_capacity', 'at_gucu', 'yurus']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Convert dates
            date_columns = ['date', 'created_at', 'updated_at']
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])
            
            # Sort by date
            if 'date' in df.columns:
                df = df.sort_values('date', ascending=False)
        
        df.to_csv(filename, index=False, encoding='utf-8')
        logger.info(f"Data saved to {filename}")
        logger.info(f"Total records saved: {len(df)}")

async def main():
    start_time = time.time()
    scraper = AutonetScraper()

    try:
        results = await scraper.scrape(start_page=1, end_page=236)
        
        if results:
            scraper.save_to_csv('autonet_final.csv')
            duration = time.time() - start_time
            logger.info(f"Successfully scraped {len(results)} listings in {duration:.2f} seconds")
            logger.info(f"Average speed: {len(results)/duration:.2f} items/second")
        else:
            logger.error("No results were scraped")
    except Exception as e:
        logger.error(f"Scraping failed: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())