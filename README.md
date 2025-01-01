# Azerbaijan Web Scrapers

A comprehensive web scraping system for collecting contact information from various Azerbaijani websites. The project includes multiple scrapers that run concurrently to gather phone numbers and associated details from classified ads and job listings.

## Features

- Concurrent scraping of multiple websites
- Robust phone number validation for Azerbaijani numbers
- PostgreSQL database integration
- Automatic retry mechanisms with random delays
- Detailed logging and statistics
- GitHub Actions integration for automated daily runs

## Supported Websites

- arenda.az (Real estate listings)
- autonet.az (Auto listings)
- birja.com (General classifieds)
- birja-in.az (General classifieds)
- boss.az (Job listings)
- emlak.az (Real estate listings)

## Requirements

- Python 3.x
- PostgreSQL database
- Required Python packages (see `requirements.txt`)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/Ismat-Samadov/numera.git
cd numera
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables in a `.env` file:
```env
DB_NAME=your_database_name
DB_USER=your_database_user
DB_PASSWORD=your_database_password
DB_HOST=your_database_host
DB_PORT=your_database_port
```

## Database Setup

Create the required database table:

```sql
CREATE TABLE leads (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    phone VARCHAR(50) UNIQUE,
    website VARCHAR(255),
    link TEXT,
    scraped_at TIMESTAMP,
    raw_data JSONB
);
```

## Phone Number Validation Rules

All phone numbers must follow these rules:
- Length must be exactly 9 digits
- Must start with valid prefixes: 10, 12, 50, 51, 55, 60, 70, 77, 99
- Fourth digit must be between 2-9 (not 0 or 1)
- Country code (994) and leading zeros are automatically removed

## Project Structure

```
.
├── LICENSE
├── README.md
├── requirements.txt
├── main.py
└── scrapers/
    ├── __init__.py
    ├── arenda.py
    ├── autonet.py
    ├── birja.py
    ├── birjain.py
    ├── boss.py
    └── emlak.py
```

## Usage

### Running the Scrapers

To run all scrapers:
```bash
python main.py
```

Each scraper can also be run individually for testing:
```bash
python -c "from scrapers.arenda import scrape; scrape()"
```

### GitHub Actions

The project includes a GitHub Actions workflow that runs the scrapers daily. The workflow:
- Runs at midnight UTC
- Can be triggered manually
- Uses repository secrets for database credentials

## Scraper Features

Each scraper includes:
- Random delays between requests
- Rotating user agents
- Automatic retries on failure
- Detailed logging
- Statistics tracking
- Connection pooling
- Concurrent processing where appropriate

### Statistics Tracked

- Total pages processed
- Total listings found
- Valid/invalid phone numbers
- Database inserts/updates
- Invalid phone number details
- Multi-phone listings (where applicable)

## Error Handling

The system includes comprehensive error handling:
- Request retries with exponential backoff
- Database transaction management
- Connection pooling
- Detailed error logging
- Graceful degradation

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests (if available)
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Notes

- The scrapers are configured to process only 3 pages by default for testing
- Adjust the `pages_to_scrape` variable in each scraper for production use
- Respect the websites' robots.txt and terms of service
- Consider implementing rate limiting for production use