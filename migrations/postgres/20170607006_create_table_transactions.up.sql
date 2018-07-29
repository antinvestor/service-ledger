CREATE TABLE transactions (
    id character varying NOT NULL,
    transacted_at timestamp without time zone NOT NULL,
    created_at timestamp default current_timestamp,
    updated_at timestamp,
    created_by character varying,
    modified_by character varying
);