BEGIN;

CREATE TABLE accounts (
    account_id SERIAL not null primary key,
    reference character varying not null,
    currency character varying not null,
    ledger_id bigint references ledgers(ledger_id),
    data jsonb default '{}'::jsonb not null,
    created_at timestamp default current_timestamp,
    updated_at timestamp,
    created_by character varying,
    modified_by character varying,
    product_id integer default 1,
    partition character varying,
    version integer default 0,
    UNIQUE (reference, product_id)
);

CREATE INDEX accounts_ledger_id_idx ON accounts USING btree (ledger_id);

CREATE INDEX accounts_data_idx ON accounts USING GIN (data jsonb_path_ops);

COMMIT;