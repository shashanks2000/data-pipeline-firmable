CREATE TABLE IF NOT EXISTS raw_data (
    id BIGSERIAL PRIMARY KEY,          -- Auto-incrementing unique ID
    file_name TEXT,
    file_data BYTEA,
    file_size INT,
    uploaded_at TIMESTAMP DEFAULT NOW()
);
