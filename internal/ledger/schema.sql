CREATE TABLE IF NOT EXISTS accounts (
 id text PRIMARY KEY,
 currency text NOT NULL CHECK(currency='USD'),
 balance bigint NOT NULL DEFAULT 0 CHECK(balance BETWEEN -1000000000000000 AND 1000000000000000),
 is_system boolean NOT NULL DEFAULT false,
 CHECK(is_system OR balance>=0)
);
CREATE TABLE IF NOT EXISTS transfers (
 id uuid PRIMARY KEY,
 idem_key text NOT NULL UNIQUE,
 fingerprint bytea NOT NULL,
 source text NOT NULL REFERENCES accounts(id),
 destination text NOT NULL REFERENCES accounts(id),
 amount bigint NOT NULL CHECK(amount>0 AND amount<=1000000000000),
 currency text NOT NULL CHECK(currency='USD'),
 state text NOT NULL DEFAULT 'accepted' CHECK(state IN ('accepted','posted','rejected')),
 reason text NOT NULL DEFAULT '',
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK(source<>destination)
);
CREATE TABLE IF NOT EXISTS outbox (
 event_id uuid PRIMARY KEY REFERENCES transfers(id),
 payload jsonb NOT NULL,
 attempts integer NOT NULL DEFAULT 0,
 next_attempt timestamptz NOT NULL DEFAULT clock_timestamp(),
 lease_token uuid,
 lease_until timestamptz,
 published_at timestamptz,
 last_error text NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS deliveries (
 id bigserial PRIMARY KEY,
 event_id uuid NOT NULL,
 payload jsonb NOT NULL,
 state text NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','done','dead')),
 attempts integer NOT NULL DEFAULT 0,
 next_attempt timestamptz NOT NULL DEFAULT clock_timestamp(),
 lease_token uuid,
 lease_until timestamptz,
 last_error text NOT NULL DEFAULT '',
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS pending_deliveries ON deliveries(next_attempt) WHERE state='pending';
CREATE TABLE IF NOT EXISTS inbox (
 event_id uuid PRIMARY KEY REFERENCES transfers(id),
 fingerprint bytea NOT NULL,
 outcome text NOT NULL CHECK(outcome IN ('posted','rejected')),
 processed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS journals (
 id uuid PRIMARY KEY,
 transfer_id uuid UNIQUE REFERENCES transfers(id),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 creator_xid xid8 NOT NULL DEFAULT pg_current_xact_id()
);
CREATE TABLE IF NOT EXISTS entries (
 id bigserial PRIMARY KEY,
 journal_id uuid NOT NULL REFERENCES journals(id),
 account_id text NOT NULL REFERENCES accounts(id),
 amount bigint NOT NULL CHECK(amount<>0)
);
CREATE INDEX IF NOT EXISTS entries_account ON entries(account_id);
CREATE TABLE IF NOT EXISTS audit (
 id bigserial PRIMARY KEY,
 transfer_id uuid,
 kind text NOT NULL,
 details jsonb NOT NULL DEFAULT '{}',
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE OR REPLACE FUNCTION immutable_ledger() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'posted ledger records are immutable' USING ERRCODE='23514'; END $$;
CREATE OR REPLACE FUNCTION entries_same_transaction() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM journals WHERE id=NEW.journal_id AND creator_xid=pg_current_xact_id()) THEN
  RAISE EXCEPTION 'cannot extend a journal from an earlier transaction' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION balanced_journal() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE n bigint; total numeric;
BEGIN
 SELECT count(*),COALESCE(sum(amount),0) INTO n,total FROM entries WHERE journal_id=NEW.id;
 IF n<2 OR total<>0 THEN RAISE EXCEPTION 'journal must have at least two entries and balance to zero' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END $$;
DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgname='journals_immutable' AND tgrelid='journals'::regclass) THEN
  CREATE TRIGGER journals_immutable BEFORE UPDATE OR DELETE ON journals FOR EACH ROW EXECUTE FUNCTION immutable_ledger();
  CREATE TRIGGER entries_immutable BEFORE UPDATE OR DELETE ON entries FOR EACH ROW EXECUTE FUNCTION immutable_ledger();
  CREATE TRIGGER entries_same_xid BEFORE INSERT ON entries FOR EACH ROW EXECUTE FUNCTION entries_same_transaction();
  CREATE CONSTRAINT TRIGGER journal_balanced AFTER INSERT ON journals DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION balanced_journal();
 END IF;
END $$;
