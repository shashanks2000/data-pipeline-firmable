CREATE TABLE IF NOT EXISTS ingestion_history (
    cc_index TEXT PRIMARY KEY,               -- e.g., Common Crawl index name or dataset id
    bookmark BIGINT,                        -- the last processed bookmark or page key
    created_at TIMESTAMP DEFAULT NOW(),   -- when this run started or was logged
    last_updated TIMESTAMP DEFAULT NOW()  -- updated when load completes or changes
);
