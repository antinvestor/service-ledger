BEGIN;

DROP VIEW IF EXISTS current_balances;
DROP VIEW IF EXISTS invalid_transactions;

ALTER TABLE entries ALTER COLUMN amount SET DATA TYPE integer;

CREATE VIEW current_balances AS
  SELECT accounts.id, accounts.data,
    COALESCE(SUM(entries.amount), 0) AS balance
  FROM accounts LEFT OUTER JOIN entries
  ON (accounts.id = entries.account_id)
  GROUP BY accounts.id;
CREATE VIEW invalid_transactions AS
  SELECT entries.transaction_id,
    sum(entries.amount) AS sum
   FROM entries
  GROUP BY entries.transaction_id
 HAVING (sum(entries.amount) > 0);

COMMIT;
