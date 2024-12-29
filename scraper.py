import aiohttp
import asyncio
import json
from typing import List, Dict, Optional
import logging
from datetime import datetime
from http.cookies import SimpleCookie
import urllib.parse
import asyncpg
import re
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
        self.auth_token = None
        self.db_pool = None

    async def _extract_auth_token(self, html_content: str) -> Optional[str]:
        """Extract authentication token from HTML content"""
        try:
            # Look for token in script tag content
            token_pattern = re.search(r'xAuthorizationToken\s*=\s*[\'"]([^\'"]+)[\'"]', html_content)
            if token_pattern:
                return token_pattern.group(1)
                
            # If not found in scripts, look for data attribute
            data_pattern = re.search(r'data-token=[\'"]([^\'"]+)[\'"]', html_content)
            if data_pattern:
                return data_pattern.group(1)

            logger.error("Could not find authentication token in page content")
            raise Exception("Authentication token not found")
            
        except Exception as e:
            logger.error(f"Error extracting auth token: {e}")
            raise

    async def _get_tokens(self, session: aiohttp.ClientSession) -> None:
        """Get CSRF token and session token"""
        try:
            headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8,ru;q=0.7,az;q=0.6",
                "Connection": "keep-alive",
                "DNT": "1",
                "Host": "autonet.az",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"macOS"'
            }
            
            async with session.get("https://autonet.az/items", headers=headers) as response:
                if response.status == 200:
                    # Get cookies
                    if 'set-cookie' in response.headers:
                        cookie = SimpleCookie()
                        for cookie_str in response.headers.getall('set-cookie', []):
                            cookie.load(cookie_str)
                            for key, morsel in cookie.items():
                                self.cookies[key] = morsel.value
                                if key == 'XSRF-TOKEN':
                                    self.cookies['XSRF-TOKEN'] = urllib.parse.unquote(morsel.value)
                    
                    logger.info("Authentication initialized successfully")
                else:
                    logger.error(f"Failed to get tokens: {response.status}")
                    raise Exception("Failed to get authentication tokens")
        except Exception as e:
            logger.error(f"Error in token acquisition: {str(e)}")
            raise

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers"""
        xsrf_token = self.cookies.get('XSRF-TOKEN', '')
        return {
            "Accept": "application/json",
            "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
            "Host": "autonet.az",
            "Referer": "https://autonet.az/items",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "X-Authorization": self.auth_token,
            "X-XSRF-TOKEN": xsrf_token
        }

    async def init_db(self):
        """Initialize database connection"""
        try:
            self.db_pool = await asyncpg.create_pool(**DB_CONFIG)
            await self.create_table()
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database initialization error: {e}")
            raise

    async def create_table(self):
        """Create the autonet_az table"""
        async with self.db_pool.acquire() as conn:
            await conn.execute('DROP TABLE IF EXISTS autonet_az')
            await conn.execute('''
                CREATE TABLE autonet_az (
                    id SERIAL PRIMARY KEY,
                    car_id INTEGER UNIQUE,
                    title TEXT,
                    price NUMERIC,
                    engine_capacity INTEGER,
                    year INTEGER,
                    mileage INTEGER,
                    make TEXT,
                    model TEXT,
                    city TEXT,
                    color INTEGER,
                    barter BOOLEAN,
                    credit BOOLEAN,
                    phone1 TEXT,
                    phone2 TEXT,
                    is_salon BOOLEAN,
                    transmission INTEGER,
                    drive_type INTEGER,
                    description TEXT,
                    created_at TIMESTAMP,
                    raw_data JSONB,
                    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            logger.info(f"Table {AUTONET_CONFIG.table_name} created successfully")

    async def _fetch_page(self, session: aiohttp.ClientSession, page: int, retry_count: int = 3) -> Dict:
        """Fetch a single page with retry mechanism"""
        for attempt in range(retry_count):
            try:
                # Construct URL with page parameter
                url = f"{self.base_url}?page={page}"
                headers = self._get_headers()
                
                async with session.get(
                    url, 
                    headers=headers,
                    cookies=self.cookies,
                    ssl=True
                ) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status in [401, 403] and attempt < retry_count - 1:
                        logger.warning(f"Authentication failed, refreshing tokens (attempt {attempt + 1})")
                        await self._get_tokens(session)
                        await asyncio.sleep(1)
                        continue
                    else:
                        logger.error(f"Error fetching page {page}: {response.status}")
                        return None
                        
            except Exception as e:
                if attempt < retry_count - 1:
                    logger.warning(f"Error on attempt {attempt + 1} for page {page}: {str(e)}")
                    await asyncio.sleep(2 ** attempt)
                    continue
                logger.error(f"Final error on page {page}: {str(e)}")
                return None
                
        return None

    async def insert_data(self, items: List[Dict]):
        """Insert data into database"""
        if not items:
            return

        async with self.db_pool.acquire() as conn:
            try:
                insert_query = '''
                    INSERT INTO autonet_az (
                        car_id, title, price, engine_capacity, year,
                        mileage, make, model, city, color,
                        barter, credit, phone1, phone2, is_salon,
                        transmission, drive_type, description, created_at, raw_data
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 
                             $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
                    ON CONFLICT (car_id) DO UPDATE SET
                        price = EXCLUDED.price,
                        mileage = EXCLUDED.mileage,
                        description = EXCLUDED.description,
                        raw_data = EXCLUDED.raw_data
                '''
                
                values = []
                for item in items:
                    created_at = datetime.fromisoformat(item.get('created_at').replace('Z', '+00:00')) if item.get('created_at') else None
                    title = f"{item.get('make', '')} {item.get('model', '')}".strip()
                    
                    values.append((
                        item.get('id'),
                        title,
                        item.get('price'),
                        item.get('engine_capacity'),
                        item.get('buraxilis_ili'),
                        item.get('yurus'),
                        item.get('make'),
                        item.get('model'),
                        item.get('cityName'),
                        item.get('rengi'),
                        item.get('barter') == 1,
                        item.get('kredit') == 1,
                        item.get('phone1'),
                        item.get('phone2'),
                        item.get('isSalon') == 1,
                        item.get('suret_qutusu'),
                        item.get('oturuculuk'),
                        item.get('information'),
                        created_at,
                        json.dumps(item)
                    ))

                await conn.executemany(insert_query, values)
                logger.info(f"Successfully inserted {len(items)} records")
                
            except Exception as e:
                logger.error(f"Error inserting data: {str(e)}")
                raise

    async def scrape(self, start_page: int = 1, end_page: int = 3) -> List[Dict]:
        """Scrape pages and store in database"""
        try:
            logger.info(f"Starting scrape from page {start_page} to {end_page}")
            await self.init_db()
            
            session_timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=session_timeout) as session:
                await self._get_tokens(session)
                
                for page in range(start_page, end_page + 1):
                    data = await self._fetch_page(session, page)
                    if data and "data" in data:
                        await self.insert_data(data["data"])
                        self.results.extend(data["data"])
                        logger.info(f"Successfully scraped page {page}")
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