CREATE VIEW current_balances AS
SELECT accounts.id, accounts.data,
    COALESCE(SUM(entries.amount), 0) AS balance
  FROM accounts LEFT OUTER JOIN entries
  ON (accounts.id = entries.account_id)
  GROUP BY accounts.id;