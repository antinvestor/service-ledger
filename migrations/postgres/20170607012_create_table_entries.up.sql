BEGIN;

CREATE TABLE entries (
    entry_id SERIAL NOT NULL PRIMARY KEY,
    transaction_id bigint NOT NULL REFERENCES transactions(transaction_id),
    account_id bigint NOT NULL REFERENCES accounts(account_id),
    amount bigint NOT NULL,
    credit boolean DEFAULT FALSE,
    account_balance bigint default 0,
    created_at timestamp default current_timestamp,
    updated_at timestamp,
    created_by character varying,
    modified_by character varying,
    product_id integer default 1,
    partition character varying,
    version integer default 0
);

CREATE INDEX entries_transaction_id_idx ON entries USING btree (transaction_id);
CREATE INDEX entries_account_id_idx ON entries USING btree (account_id);

COMMIT;