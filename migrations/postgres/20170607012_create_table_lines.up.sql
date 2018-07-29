CREATE TABLE entries (
    id bigint NOT NULL,
    transaction_id character varying NOT NULL,
    account_id character varying NOT NULL,
    amount integer NOT NULL,
    account_balance integer NOT NULL,
    created_at timestamp default current_timestamp,
    updated_at timestamp,
    created_by character varying,
    modified_by character varying
);