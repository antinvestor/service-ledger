
CREATE TABLE account_balances_view_refresh_log (
      last_refresh TIMESTAMP
);


CREATE OR REPLACE FUNCTION refresh_account_balances_view() RETURNS void AS $$
BEGIN
    -- Refresh materialized view incrementally
    REFRESH MATERIALIZED VIEW CONCURRENTLY account_balances_view;
    -- Update the last refresh timestamp
UPDATE account_balances_view_refresh_log SET last_refresh = NOW();
END;
$$ LANGUAGE plpgsql;

