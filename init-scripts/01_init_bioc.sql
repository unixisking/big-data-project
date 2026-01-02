-- Create the table if it doesn't exist
CREATE TABLE IF NOT EXISTS bioc_data (
    id SERIAL PRIMARY KEY,
    document JSONB
);

-- Create an index to speed up PMID lookups later
CREATE INDEX idx_pmid ON bioc_data ((document->'passages'->0->'infons'->>'article-id_pmid'));