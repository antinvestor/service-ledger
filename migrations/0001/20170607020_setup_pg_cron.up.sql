
-- -- Install pg_cron extension if not already installed
-- CREATE EXTENSION IF NOT EXISTS pg_cron;
--
-- -- Schedule the refresh function to run every hour
-- SELECT cron.schedule('0 * * * *', 'SELECT refresh_account_balances_view();');
