CREATE VIEW invalid_transactions AS
 SELECT entries.transaction_id,
    sum(entries.amount) AS sum
   FROM entries
  GROUP BY entries.transaction_id
 HAVING (sum(entries.amount) > 0);