BEGIN;

DROP INDEX IF EXISTS ledgers_data_idx;
DROP INDEX IF EXISTS ledgers_reference_idx;
DROP INDEX IF EXISTS ledgers_parent_ledger_id_idx;
DROP TABLE IF EXISTS ledgers;

COMMIT;