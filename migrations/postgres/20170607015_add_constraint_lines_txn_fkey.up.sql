ALTER TABLE ONLY entries
    ADD CONSTRAINT entries_txn_fkey FOREIGN KEY (transaction_id) REFERENCES transactions(id);