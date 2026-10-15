package ledger

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

type Store struct{ Pool *pgxpool.Pool }

func Open(ctx context.Context, url string) (*Store, error) {
	c, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("invalid database configuration")
	}
	c.MaxConns = 8
	c.ConnConfig.ConnectTimeout = 3 * time.Second
	p, err := pgxpool.NewWithConfig(ctx, c)
	if err != nil {
		return nil, err
	}
	return &Store{p}, nil
}
func (s *Store) Migrate(ctx context.Context) error { _, err := s.Pool.Exec(ctx, schema); return err }

func (s *Store) Seed(ctx context.Context, opening int64) error {
	if opening < 1 || opening > MaxBalance/3 {
		return ErrInvalid
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(417291)"); err != nil {
		return err
	}
	openingID := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	var exists bool
	if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM journals WHERE id=$1)", openingID).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return tx.Commit(ctx)
	}
	for _, id := range []string{"equity", "alice", "bob", "carol"} {
		if _, err = tx.Exec(ctx, "INSERT INTO accounts(id,currency,is_system) VALUES($1,'USD',$2)", id, id == "equity"); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(ctx, "INSERT INTO journals(id) VALUES($1)", openingID); err != nil {
		return err
	}
	for _, id := range []string{"equity", "alice", "bob", "carol"} {
		amount := opening
		if id == "equity" {
			amount = -3 * opening
		}
		if _, err = tx.Exec(ctx, "INSERT INTO entries(journal_id,account_id,amount) VALUES($1,$2,$3)", openingID, id, amount); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, "UPDATE accounts SET balance=$2 WHERE id=$1", id, amount); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) Accept(ctx context.Context, key string, r Request) (Transfer, bool, error) {
	if err := ValidateKey(key); err != nil {
		return Transfer{}, false, err
	}
	if err := r.Validate(); err != nil {
		return Transfer{}, false, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Transfer{}, false, err
	}
	defer tx.Rollback(context.Background())
	var count int
	if err = tx.QueryRow(ctx, "SELECT count(*) FROM accounts WHERE id IN ($1,$2) AND currency=$3 AND NOT is_system", r.From, r.To, r.Currency).Scan(&count); err != nil {
		return Transfer{}, false, err
	}
	if count != 2 {
		return Transfer{}, false, fmt.Errorf("%w: two registered customer accounts required", ErrInvalid)
	}
	id := uuid.New()
	var inserted uuid.UUID
	err = tx.QueryRow(ctx, "INSERT INTO transfers(id,idem_key,fingerprint,source,destination,amount,currency) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(idem_key) DO NOTHING RETURNING id", id, key, r.Hash(), r.From, r.To, r.Amount, r.Currency).Scan(&inserted)
	if errors.Is(err, pgx.ErrNoRows) {
		var old []byte
		var existing Transfer
		err = tx.QueryRow(ctx, "SELECT id,fingerprint,source,destination,amount,currency,state,reason,created_at FROM transfers WHERE idem_key=$1", key).Scan(&existing.ID, &old, &existing.Request.From, &existing.Request.To, &existing.Request.Amount, &existing.Request.Currency, &existing.State, &existing.Reason, &existing.CreatedAt)
		if err != nil {
			return Transfer{}, false, err
		}
		if !bytes.Equal(old, r.Hash()) {
			return Transfer{}, false, ErrConflict
		}
		return existing, true, tx.Commit(ctx)
	}
	if err != nil {
		return Transfer{}, false, err
	}
	e := Event{Version: 1, ID: id, Request: r}
	if _, err = tx.Exec(ctx, "INSERT INTO outbox(event_id,payload) VALUES($1,$2)", id, e.Bytes()); err != nil {
		return Transfer{}, false, err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO audit(transfer_id,kind) VALUES($1,'accepted')", id); err != nil {
		return Transfer{}, false, err
	}
	var created time.Time
	if err = tx.QueryRow(ctx, "SELECT created_at FROM transfers WHERE id=$1", id).Scan(&created); err != nil {
		return Transfer{}, false, err
	}
	return Transfer{ID: id, Request: r, State: "accepted", CreatedAt: created}, false, tx.Commit(ctx)
}
func (s *Store) Get(ctx context.Context, id uuid.UUID) (Transfer, error) {
	var t Transfer
	err := s.Pool.QueryRow(ctx, "SELECT id,source,destination,amount,currency,state,reason,created_at FROM transfers WHERE id=$1", id).Scan(&t.ID, &t.Request.From, &t.Request.To, &t.Request.Amount, &t.Request.Currency, &t.State, &t.Reason, &t.CreatedAt)
	return t, err
}
func (s *Store) Accounts(ctx context.Context) ([]Account, error) {
	rows, err := s.Pool.Query(ctx, "SELECT id,currency,balance,is_system FROM accounts ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Account{}
	for rows.Next() {
		var a Account
		if err = rows.Scan(&a.ID, &a.Currency, &a.Balance, &a.System); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Store) Process(ctx context.Context, e Event) (string, error) {
	if err := e.Validate(); err != nil {
		return "", err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(context.Background())
	var expected []byte
	var r Request
	var state string
	err = tx.QueryRow(ctx, "SELECT fingerprint,source,destination,amount,currency,state FROM transfers WHERE id=$1 FOR UPDATE", e.ID).Scan(&expected, &r.From, &r.To, &r.Amount, &r.Currency, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("%w: unregistered transfer", ErrPermanent)
	}
	if err != nil {
		return "", err
	}
	if !bytes.Equal(expected, e.Request.Hash()) {
		return "", fmt.Errorf("%w: event disagrees with registered transfer", ErrPermanent)
	}
	var outcome string
	err = tx.QueryRow(ctx, "SELECT outcome FROM inbox WHERE event_id=$1", e.ID).Scan(&outcome)
	if err == nil {
		return "duplicate", tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}
	if state != "accepted" {
		return "", fmt.Errorf("%w: terminal transfer has no inbox record", ErrPermanent)
	}
	rows, err := tx.Query(ctx, "SELECT id,balance FROM accounts WHERE id IN ($1,$2) ORDER BY id FOR UPDATE", r.From, r.To)
	if err != nil {
		return "", err
	}
	balances := map[string]int64{}
	for rows.Next() {
		var id string
		var balance int64
		if err = rows.Scan(&id, &balance); err != nil {
			rows.Close()
			return "", err
		}
		balances[id] = balance
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return "", err
	}
	if len(balances) != 2 {
		return "", fmt.Errorf("%w: missing account", ErrPermanent)
	}
	reason := ""
	if balances[r.From] < r.Amount {
		reason = "insufficient_funds"
	}
	if balances[r.To] > MaxBalance-r.Amount {
		reason = "destination_limit"
	}
	outcome = "posted"
	if reason != "" {
		outcome = "rejected"
	}
	if _, err = tx.Exec(ctx, "INSERT INTO inbox(event_id,fingerprint,outcome) VALUES($1,$2,$3)", e.ID, expected, outcome); err != nil {
		return "", err
	}
	if outcome == "posted" {
		journalID := uuid.New()
		if _, err = tx.Exec(ctx, "INSERT INTO journals(id,transfer_id) VALUES($1,$2)", journalID, e.ID); err != nil {
			return "", err
		}
		if _, err = tx.Exec(ctx, "INSERT INTO entries(journal_id,account_id,amount) VALUES($1,$2,$3),($1,$4,$5)", journalID, r.From, -r.Amount, r.To, r.Amount); err != nil {
			return "", err
		}
		if _, err = tx.Exec(ctx, "UPDATE accounts SET balance=balance+$2 WHERE id=$1", r.From, -r.Amount); err != nil {
			return "", err
		}
		if _, err = tx.Exec(ctx, "UPDATE accounts SET balance=balance+$2 WHERE id=$1", r.To, r.Amount); err != nil {
			return "", err
		}
	}
	if _, err = tx.Exec(ctx, "UPDATE transfers SET state=$2,reason=$3 WHERE id=$1", e.ID, outcome, reason); err != nil {
		return "", err
	}
	details, _ := json.Marshal(map[string]string{"reason": reason})
	if _, err = tx.Exec(ctx, "INSERT INTO audit(transfer_id,kind,details) VALUES($1,$2,$3)", e.ID, outcome, details); err != nil {
		return "", err
	}
	return outcome, tx.Commit(ctx)
}

func (s *Store) Reconcile(ctx context.Context) (Reconciliation, error) {
	// A repeatable-read snapshot avoids comparing account and journal states from different commits.
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return Reconciliation{}, err
	}
	defer tx.Rollback(context.Background())
	r := Reconciliation{AccountDrift: []Drift{}}
	rows, err := tx.Query(ctx, "SELECT a.id,a.balance,COALESCE(sum(e.amount),0)::bigint FROM accounts a LEFT JOIN entries e ON e.account_id=a.id GROUP BY a.id HAVING a.balance<>COALESCE(sum(e.amount),0) ORDER BY a.id")
	if err != nil {
		return r, err
	}
	for rows.Next() {
		var d Drift
		if err = rows.Scan(&d.Account, &d.Cached, &d.Journal); err != nil {
			rows.Close()
			return r, err
		}
		r.AccountDrift = append(r.AccountDrift, d)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return r, err
	}
	if err = tx.QueryRow(ctx, "SELECT count(*) FROM (SELECT j.id FROM journals j LEFT JOIN entries e ON e.journal_id=j.id GROUP BY j.id HAVING count(e.id)<2 OR COALESCE(sum(e.amount),0)<>0) bad").Scan(&r.UnbalancedJournals); err != nil {
		return r, err
	}
	if err = tx.QueryRow(ctx, "SELECT count(*) FROM transfers t LEFT JOIN journals j ON j.transfer_id=t.id WHERE t.state='posted' AND j.id IS NULL").Scan(&r.MissingJournals); err != nil {
		return r, err
	}
	if err = tx.QueryRow(ctx, "SELECT count(*) FROM journals j LEFT JOIN transfers t ON t.id=j.transfer_id WHERE j.transfer_id IS NOT NULL AND t.state IS DISTINCT FROM 'posted'").Scan(&r.UnexpectedJournals); err != nil {
		return r, err
	}
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM (
	 SELECT j.id,t.amount,count(e.id) AS n,
	 COALESCE(sum(e.amount) FILTER(WHERE e.account_id=t.source),0) AS source_amount,
	 COALESCE(sum(e.amount) FILTER(WHERE e.account_id=t.destination),0) AS destination_amount
	 FROM journals j JOIN transfers t ON t.id=j.transfer_id LEFT JOIN entries e ON e.journal_id=j.id
	 GROUP BY j.id,t.id
	 ) posted WHERE n<>2 OR source_amount<>-amount OR destination_amount<>amount`).Scan(&r.MismatchedJournals); err != nil {
		return r, err
	}
	if err = tx.QueryRow(ctx, "SELECT count(*) FROM (SELECT transfer_id FROM journals WHERE transfer_id IS NOT NULL GROUP BY transfer_id HAVING count(*)<>1) bad").Scan(&r.DuplicateJournals); err != nil {
		return r, err
	}
	r.Balanced = len(r.AccountDrift) == 0 && r.UnbalancedJournals == 0 && r.MissingJournals == 0 && r.UnexpectedJournals == 0 && r.MismatchedJournals == 0 && r.DuplicateJournals == 0
	return r, tx.Commit(ctx)
}
