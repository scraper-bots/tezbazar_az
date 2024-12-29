drop table leads;

CREATE TABLE leads (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    phone VARCHAR(50) UNIQUE,
    website VARCHAR(255),
    link TEXT,
    scraped_at TIMESTAMP,
    raw_data JSONB
);