CREATE VIEW invalid_transactions AS
 SELECT e.transaction_id,
    sum(e.amount) AS sum
   FROM transaction_entries e
  GROUP BY e.transaction_id
 HAVING (sum(e.amount) > 0);