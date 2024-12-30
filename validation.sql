WITH phone_analysis AS (
    SELECT 
        id,
        phone,
        website,
        -- Clean phone number (remove non-digits)
        REGEXP_REPLACE(phone, '[^0-9]', '', 'g') as clean_phone,
        -- Extract provider code (positions 4-5 after 994)
        SUBSTRING(REGEXP_REPLACE(phone, '[^0-9]', '', 'g') FROM 4 FOR 2) as provider_code,
        -- Extract subscriber number (everything after provider code)
        SUBSTRING(REGEXP_REPLACE(phone, '[^0-9]', '', 'g') FROM 6) as subscriber_number
    FROM 
        public.leads
    WHERE 
        phone IS NOT NULL
)
SELECT 
    website,
    COUNT(*) as total_numbers,
    SUM(CASE 
        WHEN 
            LENGTH(clean_phone) = 12 
            AND clean_phone LIKE '994%'
            AND provider_code IN ('50', '51', '55', '70', '77', '99', '10', '60', '12')
            AND LEFT(subscriber_number, 1) NOT IN ('0', '1')
        THEN 1 
        ELSE 0 
    END) as valid_numbers,
    SUM(CASE 
        WHEN 
            LENGTH(clean_phone) = 12 
            AND clean_phone LIKE '994%'
            AND provider_code IN ('50', '51', '55', '70', '77', '99', '10', '60', '12')
            AND LEFT(subscriber_number, 1) NOT IN ('0', '1')
        THEN 0 
        ELSE 1 
    END) as invalid_numbers,
    provider_code,
    COUNT(*) as count_per_provider
FROM 
    phone_analysis
GROUP BY 
    website, provider_code
ORDER BY 
    website, count_per_provider DESC;