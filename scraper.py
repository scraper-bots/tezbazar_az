import aiohttp
import asyncio
import json
from typing import List, Dict
import logging
import pandas as pd
from datetime import datetime
from http.cookies import SimpleCookie
import urllib.parse
import asyncpg
from config import DB_CONFIG, AUTONET_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AutonetScraper:
    def __init__(self):
        self.base_url = AUTONET_CONFIG.url
        self.results = []
        self.cookies = {}
        self.x_auth_token = "00028c2ddcc1ca6c32bc919dca64c288bf32ff2a"
        self.db_pool = None

    async def init_db(self):
        """Initialize database connection pool and create table if not exists"""
        try:
            self.db_pool = await asyncpg.create_pool(**DB_CONFIG)
            await self.create_table()
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database initialization error: {e}")
            raise

    async def create_table(self):
        """Create the autonet_cars table if it doesn't exist"""
        async with self.db_pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS autonet_cars (
                    id BIGINT PRIMARY KEY,
                    price NUMERIC,
                    engine_capacity INTEGER,
                    at_gucu INTEGER,
                    yurus INTEGER,
                    date TIMESTAMP,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    marka TEXT,
                    model TEXT,
                    ban_type TEXT,
                    color TEXT,
                    fuel_type TEXT,
                    transmission TEXT,
                    gear TEXT,
                    condition TEXT,
                    description TEXT,
                    city TEXT,
                    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            logger.info(f"Table {AUTONET_CONFIG.table_name} created or already exists")

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
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8,ru;q=0.7,az;q=0.6",
            "Connection": "keep-alive",
            "Host": "autonet.az",
            "Referer": "https://autonet.az/items",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "X-Authorization": self.x_auth_token,
            "X-XSRF-TOKEN": xsrf_token
        }

    async def _fetch_page(self, session: aiohttp.ClientSession, page: int) -> Dict:
        """Fetch a single page of results"""
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
                    return await response.json()
                else:
                    logger.error(f"Error fetching page {page}: {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Error on page {page}: {str(e)}")
            return None

    async def insert_data(self, items: List[Dict]):
        """Insert scraped data into database"""
        if not items:
            return

        async with self.db_pool.acquire() as conn:
            try:
                # Create a template for the insert query
                insert_query = '''
                    INSERT INTO autonet_cars (
                        id, price, engine_capacity, at_gucu, yurus,
                        date, created_at, updated_at, marka, model,
                        ban_type, color, fuel_type, transmission, gear,
                        condition, description, city
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 
                             $11, $12, $13, $14, $15, $16, $17, $18)
                    ON CONFLICT (id) DO UPDATE SET
                        price = EXCLUDED.price,
                        updated_at = EXCLUDED.updated_at,
                        scraped_at = CURRENT_TIMESTAMP
                '''
                
                # Prepare data for insertion
                values = []
                for item in items:
                    values.append((
                        item.get('id'),
                        item.get('price'),
                        item.get('engine_capacity'),
                        item.get('at_gucu'),
                        item.get('yurus'),
                        item.get('date'),
                        item.get('created_at'),
                        item.get('updated_at'),
                        item.get('marka'),
                        item.get('model'),
                        item.get('ban_type'),
                        item.get('color'),
                        item.get('fuel_type'),
                        item.get('transmission'),
                        item.get('gear'),
                        item.get('condition'),
                        item.get('description'),
                        item.get('city')
                    ))

                await conn.executemany(insert_query, values)
                logger.info(f"Successfully inserted {len(items)} records")
                
            except Exception as e:
                logger.error(f"Error inserting data: {str(e)}")
                raise

    async def scrape(self, start_page: int = 1, end_page: int = 3) -> List[Dict]:
        """Scrape all pages and store in database"""
        try:
            logger.info(f"Starting scrape from page {start_page} to {end_page}")
            await self.init_db()
            
            session_timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=session_timeout) as session:
                await self._get_tokens(session)
                
                test_data = await self._fetch_page(session, 1)
                if not test_data:
                    raise Exception("Initial test request failed")
                
                for page in range(start_page, end_page + 1):
                    data = await self._fetch_page(session, page)
                    if data and "data" in data:
                        await self.insert_data(data["data"])
                        self.results.extend(data["data"])
                        logger.info(f"Successfully scraped and stored page {page}")
                        await asyncio.sleep(1)
                    else:
                        logger.error(f"Failed to get data from page {page}")

            return self.results

        except Exception as e:
            logger.error(f"Scraping failed: {str(e)}")
            raise
        finally:
            if self.db_pool:
                await self.db_pool.close()

    def save_to_csv(self, filename: str = None) -> None:
        """Save results to CSV file (backup)"""
        if not filename:
            filename = f"autonet_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        df = pd.DataFrame(self.results)
        if not df.empty:
            numeric_columns = ['price', 'id', 'engine_capacity', 'at_gucu', 'yurus']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            date_columns = ['date', 'created_at', 'updated_at']
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])
            
            if 'date' in df.columns:
                df = df.sort_values('date', ascending=False)
        
        df.to_csv(filename, index=False, encoding='utf-8')
        logger.info(f"Backup data saved to {filename}")