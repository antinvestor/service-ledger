
-- Recreate the tsvector column and handle empty jsonb_to_tsv properly
ALTER TABLE transactions
    ADD COLUMN search_properties tsvector GENERATED ALWAYS AS (
        jsonb_to_tsv(COALESCE(data, '{}'::jsonb))
        ) STORED;

-- Recreate the GIN index
CREATE INDEX idx_transactions_search_properties ON transactions USING GIN (search_properties);
