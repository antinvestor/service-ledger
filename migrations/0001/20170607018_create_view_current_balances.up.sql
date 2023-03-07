CREATE VIEW current_balances AS
SELECT a.id AS account_id, COALESCE(SUM(e.amount), 0) AS balance
  FROM accounts a LEFT OUTER JOIN transaction_entries e
  ON (a.id = e.account_id)
  GROUP BY a.id;

