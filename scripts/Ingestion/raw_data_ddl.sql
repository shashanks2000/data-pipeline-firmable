CREATE TABLE raw_data (
    id BIGSERIAL PRIMARY KEY,          -- Auto-incrementing unique ID
    data TEXT NOT NULL,                -- The string or JSON data
    created_at TIMESTAMP DEFAULT NOW() -- Timestamp of insertion
);
