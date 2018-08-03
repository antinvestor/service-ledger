
BEGIN;

DROP INDEX IF EXISTS transactions_data_idx;
DROP INDEX IF EXISTS transacted_at_idx;

DROP TABLE IF EXISTS transactions;

COMMIT;