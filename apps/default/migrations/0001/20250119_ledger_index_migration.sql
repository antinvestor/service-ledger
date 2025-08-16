
-- Recreate the tsvector column and handle empty jsonb_to_tsv properly
ALTER TABLE ledgers
    ADD COLUMN search_properties tsvector GENERATED ALWAYS AS (
        jsonb_to_tsv(COALESCE(data, '{}'::jsonb))
        ) STORED;

-- Recreate the GIN index
CREATE INDEX idx_ledgers_search_properties ON ledgers USING GIN (search_properties);
