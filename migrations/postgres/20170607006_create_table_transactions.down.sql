
BEGIN;

DROP INDEX IF EXISTS transactions_data_idx;
DROP INDEX IF EXISTS transactions_reference_idx;
DROP INDEX IF EXISTS transactions_transacted_at_idx;

DROP TABLE IF EXISTS transactions;

COMMIT;