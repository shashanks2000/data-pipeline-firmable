CREATE TABLE commoncrawl_flatten (
    id SERIAL PRIMARY KEY,

    -- Core Crawl Metadata
    urlkey TEXT NOT NULL,                -- Normalized key used by Common Crawl
    url TEXT NOT NULL,                   -- Actual crawled URL
    timestamp TIMESTAMP NOT NULL,        -- Crawl timestamp (UTC)
    status SMALLINT,                     -- HTTP status code
    mime TEXT,                           -- Declared MIME type
    mime_detected TEXT,                  -- MIME detected by Common Crawl
    digest CHAR(32),                     -- MD5-like hash of the content
    length INTEGER,                      -- Size of content in bytes

    -- Storage Details
    offset BIGINT,                       -- Offset within the WARC file
    filename TEXT,                       -- WARC file path inside S3 bucket

    -- Derived fields (optional, for analytics)
    domain TEXT GENERATED ALWAYS AS (substring(url from 'https?://([^/]+)/')) STORED,
    path TEXT GENERATED ALWAYS AS (substring(url from 'https?://[^/]+(/.*)')) STORED,

    -- Ingestion Metadata
    inserted_at TIMESTAMP DEFAULT NOW()
);
