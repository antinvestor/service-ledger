BEGIN;


CREATE TABLE ledgers (
    ledger_id SERIAL not null primary key,
    ledger_type character varying not null,
    data jsonb default '{}'::jsonb not null,
    parent_ledger_id bigint references ledgers(ledger_id),
    created_at timestamp default current_timestamp,
    updated_at timestamp,
    created_by character varying,
    modified_by character varying,
    product_id integer default 1,
    partition character varying,
    version integer default 0

);

CREATE INDEX parent_ledger_id_idx ON ledgers USING btree (parent_ledger_id);

CREATE INDEX ledgers_data_idx ON ledgers USING GIN (data jsonb_path_ops);

COMMIT;