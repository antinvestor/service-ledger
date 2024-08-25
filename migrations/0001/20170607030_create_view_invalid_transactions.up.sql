CREATE VIEW invalid_transactions AS
 SELECT e.transaction_id,
    sum(e.amount) AS sum
   FROM transaction_entries e
    LEFT JOIN transactions t ON e.transaction_id = t.id
    WHERE t.transaction_type IN ('NORMAL', 'REVERSAL')
  GROUP BY e.transaction_id
 HAVING (sum(e.amount) > 0);