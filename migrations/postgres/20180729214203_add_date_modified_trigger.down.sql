DROP TRIGGER IF EXISTS accounts_update_at ON accounts;
DROP TRIGGER IF EXISTS entries_update_at ON entries;
DROP TRIGGER IF EXISTS transactions_update_at ON transactions;
DROP FUNCTION IF EXISTS updated_at_trigger;