
CREATE MATERIALIZED VIEW account_balances_view AS
WITH balance_summary AS (
    SELECT
        e.account_id,
        t.currency,
        COALESCE(SUM(CASE WHEN t.transaction_type IN ('NORMAL', 'REVERSAL') AND t.cleared_at IS NOT NULL THEN e.amount ELSE 0 END), 0) AS balance,
        COALESCE(SUM(CASE WHEN t.transaction_type IN ('NORMAL', 'REVERSAL') AND t.cleared_at IS NULL THEN e.amount ELSE 0 END), 0) AS uncleared_balance,
        COALESCE(SUM(CASE WHEN t.transaction_type = 'RESERVATION' THEN e.amount ELSE 0 END), 0) AS reserved_balance
    FROM transaction_entries e
    LEFT JOIN transactions t ON e.transaction_id = t.id
    GROUP BY e.account_id, t.currency
)
SELECT
    a.id as account_id,
    a.currency,
    a.data,
    COALESCE(bs.balance, 0) AS balance,
    COALESCE(bs.uncleared_balance, 0) AS uncleared_balance,
    COALESCE(bs.reserved_balance, 0) AS reserved_balance,
    a.ledger_id,
    a.ledger_type,
    a.created_at,
    a.modified_at,
    a.version,
    a.tenant_id,
    a.partition_id,
    a.access_id,
    a.deleted_at
FROM accounts a
         LEFT JOIN balance_summary bs ON a.id = bs.account_id AND a.currency = bs.currency
    WITH DATA;

CREATE INDEX idx_account_balances_view_account_id_currency ON account_balances_view (account_id, currency);
