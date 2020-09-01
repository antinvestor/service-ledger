
BEGIN;

CREATE OR REPLACE FUNCTION updated_at_trigger()
RETURNS TRIGGER AS $$
BEGIN
   IF row(NEW.*) IS DISTINCT FROM row(OLD.*) THEN
      NEW.updated_at = now();
      NEW.version = OLD.version + 1;
      RETURN NEW;
   ELSE
      RETURN OLD;
   END IF;
END;
$$ language 'plpgsql';

CREATE TRIGGER accounts_update_at BEFORE UPDATE ON accounts FOR EACH ROW EXECUTE PROCEDURE  updated_at_trigger();
CREATE TRIGGER entries_update_at BEFORE UPDATE ON entries FOR EACH ROW EXECUTE PROCEDURE  updated_at_trigger();
CREATE TRIGGER transactions_update_at BEFORE UPDATE ON transactions FOR EACH ROW EXECUTE PROCEDURE  updated_at_trigger();

COMMIT;