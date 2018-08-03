BEGIN;

DROP INDEX IF EXISTS entries_account_id_idx;

DROP INDEX IF EXISTS entries_transaction_id_idx;

DROP TABLE IF EXISTS entries;

COMMIT;