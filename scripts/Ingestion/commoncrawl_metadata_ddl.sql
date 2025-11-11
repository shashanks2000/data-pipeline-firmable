CREATE TABLE commoncrawl_metadata (
    id TEXT PRIMARY KEY,                 -- e.g. 'CC-MAIN-2025-43'
    name TEXT,                           -- e.g. 'October 2025 Index'
    timegate TEXT,                       -- API base URL for Wayback access
    cdx_api TEXT,                        -- API base URL for CDX index access
    crawl_start TIMESTAMP,               -- "from"
    crawl_end TIMESTAMP,                 -- "to"
    created_at TIMESTAMP DEFAULT NOW()   -- Ingestion timestamp
);
