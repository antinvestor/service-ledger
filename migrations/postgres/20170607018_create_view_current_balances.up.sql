CREATE VIEW current_balances AS
SELECT accounts.account_id, COALESCE(SUM(entries.amount), 0) AS balance
  FROM accounts LEFT OUTER JOIN entries
  ON (accounts.account_id = entries.account_id)
  GROUP BY accounts.account_id;

