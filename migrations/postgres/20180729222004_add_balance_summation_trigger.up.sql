
BEGIN;

CREATE OR REPLACE FUNCTION update_account_balance()
RETURNS TRIGGER AS $$
BEGIN
    NEW.account_balance = (SELECT SUM(e.amount) AS balance FROM transaction_entries e WHERE e.account_id = NEW.account_id );
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_account_balance_trigger BEFORE INSERT ON entries FOR EACH ROW EXECUTE PROCEDURE  update_account_balance();

COMMIT;