BEGIN;

DROP INDEX IF EXISTS accounts_data_idx;

DROP INDEX IF EXISTS parent_ledger_id_idx;

DROP TABLE IF EXISTS accounts;

COMMIT;