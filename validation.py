import psycopg2
import re
from typing import Dict, List, Tuple
import logging
from dotenv import load_dotenv
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PhoneValidator:
    VALID_PROVIDERS = {'50', '51', '55', '70', '77', '99', '10', '60', '12'}
    
    @staticmethod
    def clean_phone(phone: str) -> str:
        """Remove all non-digit characters from phone number and add + if needed"""
        cleaned = ''.join(filter(str.isdigit, phone))
        if cleaned.startswith('994'):
            return '+' + cleaned
        return cleaned
    
    @staticmethod
    def validate_phone(phone: str) -> Tuple[bool, str]:
        """
        Validate phone number according to Azerbaijan rules
        Returns: (is_valid, reason_if_invalid)
        """
        # Clean the phone number first
        cleaned = PhoneValidator.clean_phone(phone)
        
        # Check if starts with +994
        if not cleaned.startswith('+994'):
            return False, "Does not start with +994"
            
        # Check length (should be 13 characters including +)
        if len(cleaned) != 13:
            return False, f"Invalid length: {len(cleaned)} (should be 13)"
            
        # Extract provider code (positions 4-6)
        provider = cleaned[4:6]
        if provider not in PhoneValidator.VALID_PROVIDERS:
            return False, f"Invalid provider code: {provider}"
            
        # Check if subscriber number starts with 0 or 1
        subscriber = cleaned[6:]
        if subscriber[0] in {'0', '1'}:
            return False, f"Subscriber number starts with {subscriber[0]}"
            
        return True, "Valid"

def get_db_connection():
    """Create database connection using environment variables"""
    load_dotenv()
    
    return psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )

def analyze_phone_numbers() -> Dict:
    """
    Analyze all phone numbers in the database and return statistics
    """
    validator = PhoneValidator()
    stats = {
        'total': 0,
        'valid': 0,
        'invalid': 0,
        'by_provider': {},
        'error_types': {},
        'by_website': {}
    }
    
    invalid_examples = []
    
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT phone, website FROM leads WHERE phone IS NOT NULL")
            rows = cur.fetchall()
            
            stats['total'] = len(rows)
            
            for phone, website in rows:
                is_valid, reason = validator.validate_phone(phone)
                
                # Update website stats
                stats['by_website'].setdefault(website, {'total': 0, 'valid': 0, 'invalid': 0})
                stats['by_website'][website]['total'] += 1
                
                if is_valid:
                    stats['valid'] += 1
                    stats['by_website'][website]['valid'] += 1
                    
                    # Update provider stats for valid numbers
                    provider = phone[4:6] if phone.startswith('+994') else 'unknown'
                    stats['by_provider'][provider] = stats['by_provider'].get(provider, 0) + 1
                else:
                    stats['invalid'] += 1
                    stats['by_website'][website]['invalid'] += 1
                    stats['error_types'][reason] = stats['error_types'].get(reason, 0) + 1
                    
                    if len(invalid_examples) < 5:
                        invalid_examples.append((phone, reason))
                        
        # Calculate percentages
        stats['valid_percentage'] = (stats['valid'] / stats['total'] * 100) if stats['total'] > 0 else 0
        
        # Sort providers by frequency
        stats['by_provider'] = dict(sorted(stats['by_provider'].items(), 
                                         key=lambda x: x[1], 
                                         reverse=True))
        
        # Add some invalid examples
        stats['invalid_examples'] = invalid_examples
            
    except Exception as e:
        logger.error(f"Error analyzing phone numbers: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            
    return stats

def main():
    try:
        stats = analyze_phone_numbers()
        
        # Print report
        print("\nPhone Number Analysis Report")
        print("=" * 50)
        print(f"Total numbers analyzed: {stats['total']}")
        print(f"Valid numbers: {stats['valid']} ({stats['valid_percentage']:.1f}%)")
        print(f"Invalid numbers: {stats['invalid']}")
        
        print("\nBreakdown by provider:")
        for provider, count in stats['by_provider'].items():
            print(f"  {provider}: {count}")
            
        print("\nBreakdown by website:")
        for website, counts in stats['by_website'].items():
            total = counts['total']
            valid_pct = (counts['valid'] / total * 100) if total > 0 else 0
            print(f"\n{website}:")
            print(f"  Total: {total}")
            print(f"  Valid: {counts['valid']} ({valid_pct:.1f}%)")
            print(f"  Invalid: {counts['invalid']}")
            
        print("\nCommon error types:")
        for error, count in stats['error_types'].items():
            print(f"  {error}: {count}")
            
        print("\nSample invalid numbers:")
        for phone, reason in stats['invalid_examples']:
            print(f"  {phone}: {reason}")
            
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        raise

if __name__ == "__main__":
    main()