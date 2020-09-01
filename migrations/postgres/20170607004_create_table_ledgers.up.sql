BEGIN;

CREATE OR REPLACE FUNCTION generate_ledger_ref()
  RETURNS varchar AS $$
        DECLARE
        result bigint;
        our_epoch bigint := 1532390400;
        now_seconds bigint;
		BEGIN
          SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp())) INTO now_seconds;
		      result := (now_seconds - our_epoch) << 15;
          result := result | (1 + random()*10^14) :: bigint;
		      RETURN base36_encode(result);

    END;
$$ LANGUAGE 'plpgsql' IMMUTABLE;


CREATE TABLE ledgers (
    ledger_id SERIAL not null primary key,
    reference character varying default generate_ledger_ref() not null,
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

CREATE INDEX ledgers_parent_ledger_id_idx ON ledgers USING btree (parent_ledger_id);
CREATE INDEX ledgers_reference_idx ON ledgers USING hash (reference);
CREATE INDEX ledgers_data_idx ON ledgers USING GIN (data jsonb_path_ops);

COMMIT;