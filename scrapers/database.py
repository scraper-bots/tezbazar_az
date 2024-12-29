import psycopg2
import psycopg2.extras
from datetime import datetime
import json
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_config: Dict):
        self.db_config = db_config
        self.ensure_table_exists()
        self.batch_size = 50
        self.leads_batch = []
        
    def connect(self):
        return psycopg2.connect(**self.db_config)
        
    def ensure_table_exists(self):
        """Create leads table if it doesn't exist"""
        create_table_query = """
            CREATE TABLE IF NOT EXISTS leads (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                phone VARCHAR(50) UNIQUE,
                website VARCHAR(255),
                link TEXT,
                scraped_at TIMESTAMP,
                raw_data JSONB
            );
        """
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_query)
                conn.commit()
    
    def remove_duplicates(self, leads: List[Dict]) -> List[Dict]:
        """Remove duplicate phone numbers keeping only the first occurrence"""
        seen_phones = set()
        unique_leads = []
        
        for lead in leads:
            phone = lead['phone']
            if phone not in seen_phones:
                seen_phones.add(phone)
                unique_leads.append(lead)
        
        return unique_leads
    
    def save_leads_batch(self, leads: List[Dict]):
        """Save multiple leads in a single transaction"""
        if not leads:
            return
            
        # Remove duplicates within the batch
        unique_leads = self.remove_duplicates(leads)
        
        # Insert or update one by one to handle conflicts
        query = """
            INSERT INTO leads (name, phone, website, link, scraped_at, raw_data)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (phone) 
            DO UPDATE SET 
                name = EXCLUDED.name,
                website = EXCLUDED.website,
                link = EXCLUDED.link,
                scraped_at = EXCLUDED.scraped_at,
                raw_data = EXCLUDED.raw_data
        """
        
        now = datetime.now()
        success_count = 0
        
        with self.connect() as conn:
            with conn.cursor() as cur:
                for lead in unique_leads:
                    try:
                        cur.execute(query, (
                            lead['name'],
                            lead['phone'],
                            lead['website'],
                            lead['link'],
                            now,
                            json.dumps(lead['raw_data'])
                        ))
                        success_count += 1
                    except Exception as e:
                        logger.error(f"Error saving lead with phone {lead['phone']}: {str(e)}")
                conn.commit()
        
        logger.info(f"Successfully saved {success_count} leads out of {len(unique_leads)} unique leads")
    
    def add_to_batch(self, lead_data: Dict):
        """Add lead to batch and save if batch size is reached"""
        self.leads_batch.append(lead_data)
        
        if len(self.leads_batch) >= self.batch_size:
            self.flush_batch()
    
    def flush_batch(self):
        """Save current batch of leads"""
        if self.leads_batch:
            self.save_leads_batch(self.leads_batch)
            self.leads_batch = []