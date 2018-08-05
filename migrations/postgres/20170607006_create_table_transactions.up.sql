BEGIN;

CREATE TABLE transactions (
    transaction_id SERIAL NOT NULL PRIMARY KEY,
    reference character varying NOT NULL,
    transacted_at timestamp without time zone NOT NULL,
    data jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp default current_timestamp,
    updated_at timestamp,
    created_by character varying,
    modified_by character varying,
    product_id integer default 1,
    partition character varying,
    version integer default 0,
    unique(reference, product_id)
);

CREATE INDEX transactions_transacted_at_idx ON transactions USING brin (transacted_at);
CREATE INDEX transactions_reference_idx ON accounts USING hash (reference);
CREATE INDEX transactions_data_idx ON transactions USING GIN (data jsonb_path_ops);

COMMIT;