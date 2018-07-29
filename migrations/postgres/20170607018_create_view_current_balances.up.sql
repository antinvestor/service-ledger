CREATE VIEW current_balances AS
 SELECT entries.account_id,
    sum(entries.amount) AS balance
   FROM entries
  GROUP BY entries.account_id;