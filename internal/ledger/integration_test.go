//go:build integration

package ledger

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testStore(t testing.TB, opening int64) *Store {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Fatal("DATABASE_URL required; integration tests cannot silently skip")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	schemaName := "it_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schemaName); err != nil {
		t.Fatal(err)
	}
	c, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	c.MaxConns = 8
	c.ConnConfig.RuntimeParams["search_path"] = schemaName
	p, err := pgxpool.NewWithConfig(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	s := &Store{p}
	t.Cleanup(func() {
		p.Close()
		admin.Exec(context.Background(), "DROP SCHEMA "+schemaName+" CASCADE")
		admin.Close()
	})
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err = s.Migrate(ctx); err != nil {
		t.Fatal("migration repeat", err)
	}
	if err = s.Seed(ctx, opening); err != nil {
		t.Fatal(err)
	}
	if err = s.Seed(ctx, opening); err != nil {
		t.Fatal("seed repeat", err)
	}
	return s
}
func accepted(t testing.TB, s *Store, key string, r Request) Event {
	t.Helper()
	tr, _, err := s.Accept(context.Background(), key, r)
	if err != nil {
		t.Fatal(err)
	}
	return Event{1, tr.ID, r}
}
func scalar(t testing.TB, s *Store, q string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := s.Pool.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func balanced(t testing.TB, s *Store) {
	t.Helper()
	r, err := s.Reconcile(context.Background())
	if err != nil || !r.Balanced {
		t.Fatalf("reconciliation failed: %+v error=%v", r, err)
	}
}

func TestConcurrentIdempotencyAcceptance(t *testing.T) {
	s := testStore(t, 100)
	ctx := context.Background()
	var wg sync.WaitGroup
	ids := make(chan uuid.UUID, 32)
	errs := make(chan error, 32)
	for range 32 {
		wg.Add(1)
		go func() { defer wg.Done(); tr, _, err := s.Accept(ctx, "same-key", example()); ids <- tr.ID; errs <- err }()
	}
	wg.Wait()
	close(ids)
	close(errs)
	var first uuid.UUID
	for id := range ids {
		if first == uuid.Nil {
			first = id
		}
		if id != first {
			t.Fatal("idempotency diverged")
		}
	}
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if scalar(t, s, "SELECT count(*) FROM transfers") != 1 || scalar(t, s, "SELECT count(*) FROM outbox") != 1 || scalar(t, s, "SELECT count(*) FROM audit WHERE kind='accepted'") != 1 {
		t.Fatal("acceptance was not atomic and idempotent")
	}
	r := example()
	r.Amount++
	if _, _, err := s.Accept(ctx, "same-key", r); err != ErrConflict {
		t.Fatal("conflicting key accepted", err)
	}
}

func TestBalancedImmutableLedger(t *testing.T) {
	s := testStore(t, 100)
	ctx := context.Background()
	e := accepted(t, s, "posted", Request{"alice", "bob", 25, "USD"})
	if outcome, err := s.Process(ctx, e); err != nil || outcome != "posted" {
		t.Fatal(outcome, err)
	}
	balanced(t, s)
	for _, q := range []string{"UPDATE entries SET amount=amount+1", "DELETE FROM entries", "UPDATE journals SET created_at=clock_timestamp()", "DELETE FROM journals"} {
		if _, err := s.Pool.Exec(ctx, q); err == nil {
			t.Fatal("immutable ledger accepted", q)
		}
	}
	var jid uuid.UUID
	if err := s.Pool.QueryRow(ctx, "SELECT id FROM journals WHERE transfer_id=$1", e.ID).Scan(&jid); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool.Exec(ctx, "INSERT INTO entries(journal_id,account_id,amount) VALUES($1,'alice',1)", jid); err == nil {
		t.Fatal("extended old journal")
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	id := uuid.New()
	tx.Exec(ctx, "INSERT INTO journals(id) VALUES($1)", id)
	tx.Exec(ctx, "INSERT INTO entries(journal_id,account_id,amount) VALUES($1,'alice',1)", id)
	if err = tx.Commit(ctx); err == nil {
		t.Fatal("unbalanced journal committed")
	}
	balanced(t, s)
}

func TestRejectionAndTransactionalRollback(t *testing.T) {
	s := testStore(t, 100)
	ctx := context.Background()
	e := accepted(t, s, "insufficient", Request{"alice", "bob", 101, "USD"})
	if outcome, err := s.Process(ctx, e); err != nil || outcome != "rejected" {
		t.Fatal(outcome, err)
	}
	if scalar(t, s, "SELECT balance FROM accounts WHERE id='alice'") != 100 || scalar(t, s, "SELECT count(*) FROM journals") != 1 {
		t.Fatal("rejection mutated ledger")
	}
	e = accepted(t, s, "rollback", Request{"alice", "bob", 25, "USD"})
	if _, err := s.Pool.Exec(ctx, "ALTER TABLE audit ADD CONSTRAINT injected_failure CHECK(kind<>'posted')"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Process(ctx, e); err == nil {
		t.Fatal("injected failure did not fire")
	}
	if scalar(t, s, "SELECT count(*) FROM inbox WHERE event_id=$1", e.ID) != 0 || scalar(t, s, "SELECT count(*) FROM journals") != 1 || scalar(t, s, "SELECT balance FROM accounts WHERE id='alice'") != 100 {
		t.Fatal("partial posting survived failed transaction")
	}
	tr, err := s.Get(ctx, e.ID)
	if err != nil || tr.State != "accepted" {
		t.Fatal("status partially mutated", tr, err)
	}
	balanced(t, s)
}

func TestLeaseOwnershipAndReplayAfterConsumerCrash(t *testing.T) {
	s := testStore(t, 100)
	ctx := context.Background()
	e := accepted(t, s, "lease", Request{"alice", "bob", 25, "USD"})
	c, ok, err := s.ClaimOutbox(ctx, time.Second)
	if err != nil || !ok {
		t.Fatal(c, ok, err)
	}
	if _, ok, err := s.ClaimOutbox(ctx, time.Second); err != nil || ok {
		t.Fatal("active outbox lease stolen", err)
	}
	s.Pool.Exec(ctx, "UPDATE outbox SET lease_until=clock_timestamp()-interval '1 second'")
	next, ok, err := s.ClaimOutbox(ctx, time.Second)
	if err != nil || !ok || next.Token == c.Token {
		t.Fatal("lease not reclaimed", err)
	}
	if err = s.FinishOutbox(ctx, c, true); err != ErrLeaseLost {
		t.Fatal("stale outbox owner acknowledged", err)
	}
	if err = s.FinishOutbox(ctx, next, true); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Append(ctx, e); err != nil {
		t.Fatal(err)
	}
	delivery, ok, err := s.ClaimDelivery(ctx, time.Second)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if _, err = s.Process(ctx, e); err != nil {
		t.Fatal(err)
	} // crash before delivery acknowledgement
	s.Pool.Exec(ctx, "UPDATE deliveries SET lease_until=clock_timestamp()-interval '1 second'")
	retry, ok, err := s.ClaimDelivery(ctx, time.Second)
	if err != nil || !ok {
		t.Fatal(err)
	}
	outcome, err := s.Process(ctx, e)
	if err != nil || outcome != "duplicate" {
		t.Fatal(outcome, err)
	}
	if err = s.FinishDelivery(ctx, delivery, nil); err != ErrLeaseLost {
		t.Fatal("stale consumer owner acknowledged", err)
	}
	if err = s.FinishDelivery(ctx, retry, nil); err != nil {
		t.Fatal(err)
	}
	if scalar(t, s, "SELECT count(*) FROM journals WHERE transfer_id=$1", e.ID) != 1 {
		t.Fatal("duplicate journal")
	}
	balanced(t, s)
}

func TestDeadLetterAndExplicitRetry(t *testing.T) {
	s := testStore(t, 100)
	ctx := context.Background()
	e := Event{1, uuid.New(), example()}
	id, err := s.Append(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = s.ConsumeOne(ctx, time.Second)
	if !errors.Is(err, ErrPermanent) {
		t.Fatal("unknown request not rejected", err)
	}
	if scalar(t, s, "SELECT count(*) FROM deliveries WHERE id=$1 AND state='dead'", id) != 1 {
		t.Fatal("dead letter missing")
	}
	if err = s.RetryDead(ctx, id); err != nil {
		t.Fatal(err)
	}
	if scalar(t, s, "SELECT attempts FROM deliveries WHERE id=$1", id) != 0 {
		t.Fatal("controlled retry did not reset attempts")
	}
	if scalar(t, s, "SELECT count(*) FROM audit WHERE kind='manual_retry'") != 1 {
		t.Fatal("retry audit missing")
	}
}

func TestAttemptLimitAndRepairRetry(t *testing.T) {
	s := testStore(t, 100)
	ctx := context.Background()
	e := accepted(t, s, "attempt-limit", Request{"alice", "bob", 25, "USD"})
	id, err := s.Append(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 8; attempt++ {
		c, ok, err := s.ClaimDelivery(ctx, time.Second)
		if err != nil || !ok || c.Attempts != attempt {
			t.Fatal("claim attempt", c, ok, err)
		}
		if err = s.FinishDelivery(ctx, c, errors.New("synthetic transient error")); err != nil {
			t.Fatal(err)
		}
		if _, err = s.Pool.Exec(ctx, "UPDATE deliveries SET next_attempt=clock_timestamp() WHERE id=$1", id); err != nil {
			t.Fatal(err)
		}
	}
	if scalar(t, s, "SELECT count(*) FROM deliveries WHERE id=$1 AND state='dead'", id) != 1 || scalar(t, s, "SELECT count(*) FROM journals WHERE transfer_id=$1", e.ID) != 0 {
		t.Fatal("attempt limit lost event or mutated ledger")
	}
	if err = s.RetryDead(ctx, id); err != nil {
		t.Fatal(err)
	}
	if ok, outcome, err := s.ConsumeOne(ctx, time.Second); err != nil || !ok || outcome != "posted" {
		t.Fatal(ok, outcome, err)
	}
	balanced(t, s)
}

func TestEventMustMatchRegisteredRequest(t *testing.T) {
	s := testStore(t, 100)
	e := accepted(t, s, "payload-integrity", Request{"alice", "bob", 25, "USD"})
	e.Request.Amount = 26
	if _, err := s.Process(context.Background(), e); !errors.Is(err, ErrPermanent) {
		t.Fatal("altered event accepted", err)
	}
	if scalar(t, s, "SELECT count(*) FROM inbox") != 0 || scalar(t, s, "SELECT count(*) FROM journals") != 1 {
		t.Fatal("rejected event mutated posting state")
	}
	balanced(t, s)
}

func TestReconciliationChecksRegisteredAmount(t *testing.T) {
	s := testStore(t, 100)
	e := accepted(t, s, "reconcile-amount", Request{"alice", "bob", 25, "USD"})
	e.Request.Amount = 24
	// A balanced but incorrect legacy posting must not pass independent reconciliation.
	if err := legacyPost(context.Background(), s, e, nil, nil); err != nil {
		t.Fatal(err)
	}
	r, err := s.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r.Balanced {
		t.Fatal("balanced wrong-amount journal escaped independent reconciliation")
	}
}

func TestRandomizedConcurrentAccounting(t *testing.T) {
	for _, seed := range []int64{7, 42, 99} {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			s := testStore(t, 100)
			rng := rand.New(rand.NewSource(seed))
			names := []string{"alice", "bob", "carol"}
			events := make([]Event, 100)
			for i := range events {
				from := rng.Intn(3)
				to := (from + 1 + rng.Intn(2)) % 3
				events[i] = accepted(t, s, fmt.Sprintf("random-%d", i), Request{names[from], names[to], int64(1 + rng.Intn(80)), "USD"})
			}
			var wg sync.WaitGroup
			errs := make(chan error, 200)
			for _, i := range rng.Perm(100) {
				for range 2 {
					wg.Add(1)
					go func(e Event) { defer wg.Done(); _, err := s.Process(context.Background(), e); errs <- err }(events[i])
				}
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}
			balanced(t, s)
			if scalar(t, s, "SELECT count(*) FROM inbox") != 100 || scalar(t, s, "SELECT sum(balance) FROM accounts WHERE NOT is_system") != 300 || scalar(t, s, "SELECT count(*) FROM accounts WHERE NOT is_system AND balance<0") != 0 {
				t.Fatal("accounting invariant failed")
			}
		})
	}
}

// legacyPost is a test-only reenactment of missing replay guards or stale absolute updates.
// It never runs in an executable service. Existing-schema hardening is explicitly removed only in the duplicate exercise.
func legacyPost(ctx context.Context, s *Store, e Event, ready chan<- struct{}, release <-chan struct{}) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var source, dest int64
	if err = tx.QueryRow(ctx, "SELECT balance FROM accounts WHERE id=$1", e.Request.From).Scan(&source); err != nil {
		return err
	}
	if err = tx.QueryRow(ctx, "SELECT balance FROM accounts WHERE id=$1", e.Request.To).Scan(&dest); err != nil {
		return err
	}
	if ready != nil {
		ready <- struct{}{}
		<-release
	}
	id := uuid.New()
	if _, err = tx.Exec(ctx, "INSERT INTO journals(id,transfer_id) VALUES($1,$2)", id, e.ID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO entries(journal_id,account_id,amount) VALUES($1,$2,$3),($1,$4,$5)", id, e.Request.From, -e.Request.Amount, e.Request.To, e.Request.Amount); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "UPDATE accounts SET balance=$2 WHERE id=$1", e.Request.From, source-e.Request.Amount); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "UPDATE accounts SET balance=$2 WHERE id=$1", e.Request.To, dest+e.Request.Amount); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "UPDATE transfers SET state='posted' WHERE id=$1", e.ID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func TestIncident001DuplicatePosting(t *testing.T) {
	s := testStore(t, 100)
	ctx := context.Background()
	e := accepted(t, s, "duplicate", Request{"alice", "bob", 25, "USD"})
	if _, err := s.Process(ctx, e); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("LF_UNSAFE_DUPLICATE") == "1" {
		if _, err := s.Pool.Exec(ctx, "ALTER TABLE journals DROP CONSTRAINT journals_transfer_id_key"); err != nil {
			t.Fatal(err)
		}
		if err := legacyPost(ctx, s, e, nil, nil); err != nil {
			t.Fatal(err)
		}
	} else {
		if outcome, err := s.Process(ctx, e); err != nil || outcome != "duplicate" {
			t.Fatal(outcome, err)
		}
	}
	n := scalar(t, s, "SELECT count(*) FROM journals WHERE transfer_id=$1", e.ID)
	if n != 1 || scalar(t, s, "SELECT balance FROM accounts WHERE id='alice'") != 75 {
		t.Fatalf("duplicate delivery created duplicate business posting: journals=%d", n)
	}
	balanced(t, s)
}

func TestIncident002UncertainRelayAcknowledgement(t *testing.T) {
	s := testStore(t, 100)
	ctx := context.Background()
	e := accepted(t, s, "uncertain", Request{"alice", "bob", 25, "USD"})
	var mu sync.Mutex
	calls := 0
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		event, err := DecodeEvent(b)
		if err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		if _, err = s.Append(r.Context(), event); err != nil {
			t.Error(err)
			w.WriteHeader(503)
			return
		}
		mu.Lock()
		calls++
		first := calls == 1
		mu.Unlock()
		if first {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				conn.Close()
			}
			return
		}
		w.WriteHeader(201)
	}))
	defer relay.Close()
	_, err := s.PublishOne(ctx, relay.Client(), relay.URL, time.Second, nil)
	if err == nil {
		t.Fatal("lost relay acknowledgement not exercised")
	}
	if os.Getenv("LF_UNSAFE_PUBLISH") == "1" {
		s.Pool.Exec(ctx, "UPDATE outbox SET published_at=clock_timestamp() WHERE event_id=$1", e.ID)
	}
	if scalar(t, s, "SELECT count(*) FROM outbox WHERE published_at IS NULL") != 1 {
		t.Fatal("uncertain publication silently discarded committed pending work")
	}
	if scalar(t, s, "SELECT count(*) FROM deliveries WHERE event_id=$1", e.ID) != 1 {
		t.Fatal("relay did not durably receive first attempt")
	}
	s.Pool.Exec(ctx, "UPDATE outbox SET next_attempt=clock_timestamp()")
	if _, err = s.PublishOne(ctx, relay.Client(), relay.URL, time.Second, nil); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if ok, _, err := s.ConsumeOne(ctx, time.Second); err != nil || !ok {
			t.Fatal(ok, err)
		}
	}
	if scalar(t, s, "SELECT count(*) FROM deliveries WHERE event_id=$1", e.ID) != 2 || scalar(t, s, "SELECT count(*) FROM journals WHERE transfer_id=$1", e.ID) != 1 {
		t.Fatal("republication/consumption contract violated")
	}
	balanced(t, s)
}

func TestIncident003ConcurrentSpend(t *testing.T) {
	s := testStore(t, 100)
	ctx := context.Background()
	events := []Event{accepted(t, s, "spend-a", Request{"alice", "bob", 75, "USD"}), accepted(t, s, "spend-b", Request{"alice", "carol", 75, "USD"})}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	unsafe := os.Getenv("LF_UNSAFE_CONCURRENCY") == "1"
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	for _, e := range events {
		wg.Add(1)
		go func(e Event) {
			defer wg.Done()
			if unsafe {
				errs <- legacyPost(ctx, s, e, ready, release)
			} else {
				_, err := s.Process(ctx, e)
				errs <- err
			}
		}(e)
	}
	if unsafe {
		<-ready
		<-ready
		close(release)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	posted := scalar(t, s, "SELECT count(*) FROM transfers WHERE state='posted'")
	rejected := scalar(t, s, "SELECT count(*) FROM transfers WHERE state='rejected'")
	r, err := s.Reconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if posted != 1 || rejected != 1 || !r.Balanced {
		t.Fatalf("concurrent spend violated accounting invariant: posted=%d rejected=%d drift=%+v", posted, rejected, r.AccountDrift)
	}
}

func TestHTTPContracts(t *testing.T) {
	s := testStore(t, 100)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := httptest.NewServer(API(s, log))
	defer server.Close()
	send := func(body, key string) int {
		req, _ := http.NewRequest("POST", server.URL+"/transfers", strings.NewReader(body))
		req.Header.Set("Idempotency-Key", key)
		res, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		return res.StatusCode
	}
	valid := `{"from":"alice","to":"bob","amount_minor":25,"currency":"USD"}`
	if send(valid, "http") != 202 || send(valid, "http") != 202 || send(strings.Replace(valid, "25", "26", 1), "http") != 409 || send(valid, "") != 400 || send(strings.Repeat("x", MaxPayload+1), "large") != 400 {
		t.Fatal("HTTP acceptance contracts failed")
	}
	response, err := server.Client().Get(server.URL + "/reconcile")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal("HTTP reconciliation failed")
	}
}

func BenchmarkAcceptAndPost(b *testing.B) {
	s := testStore(b, MaxBalance/10)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := accepted(b, s, fmt.Sprintf("bench-%d", i), Request{"alice", "bob", 1, "USD"})
		if _, err := s.Process(ctx, e); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	balanced(b, s)
}
