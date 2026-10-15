package ledger

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"time"
)

func (s *Store) ClaimOutbox(ctx context.Context, lease time.Duration) (Claim, bool, error) {
	c := Claim{Token: uuid.New()}
	err := s.Pool.QueryRow(ctx, `WITH candidate AS (
 SELECT event_id FROM outbox WHERE published_at IS NULL AND next_attempt<=clock_timestamp()
 AND (lease_until IS NULL OR lease_until<clock_timestamp()) ORDER BY next_attempt,event_id FOR UPDATE SKIP LOCKED LIMIT 1)
 UPDATE outbox o SET lease_token=$1,lease_until=clock_timestamp()+$2::interval,attempts=o.attempts+1 FROM candidate c
 WHERE o.event_id=c.event_id RETURNING o.event_id,o.payload,o.attempts`, c.Token, lease.String()).Scan(&c.EventID, &c.Payload, &c.Attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, false, nil
	}
	return c, err == nil, err
}
func (s *Store) FinishOutbox(ctx context.Context, c Claim, success bool) error {
	query := "UPDATE outbox SET published_at=clock_timestamp(),lease_token=NULL,lease_until=NULL,last_error='' WHERE event_id=$1 AND lease_token=$2"
	args := []any{c.EventID, c.Token}
	if !success {
		query = "UPDATE outbox SET lease_token=NULL,lease_until=NULL,next_attempt=clock_timestamp()+$3::interval,last_error='relay_unavailable' WHERE event_id=$1 AND lease_token=$2"
		args = append(args, RetryDelay(c.Attempts).String())
	}
	result, err := s.Pool.Exec(ctx, query, args...)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}
func (s *Store) Append(ctx context.Context, e Event) (int64, error) {
	if err := e.Validate(); err != nil {
		return 0, err
	}
	var id int64
	err := s.Pool.QueryRow(ctx, "INSERT INTO deliveries(event_id,payload) VALUES($1,$2) RETURNING id", e.ID, e.Bytes()).Scan(&id)
	return id, err
}
func (s *Store) ClaimDelivery(ctx context.Context, lease time.Duration) (Claim, bool, error) {
	c := Claim{Token: uuid.New()}
	err := s.Pool.QueryRow(ctx, `WITH candidate AS (
 SELECT id FROM deliveries WHERE state='pending' AND next_attempt<=clock_timestamp()
 AND (lease_until IS NULL OR lease_until<clock_timestamp()) ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1)
 UPDATE deliveries d SET lease_token=$1,lease_until=clock_timestamp()+$2::interval,attempts=d.attempts+1 FROM candidate c
 WHERE d.id=c.id RETURNING d.id,d.event_id,d.payload,d.attempts`, c.Token, lease.String()).Scan(&c.ID, &c.EventID, &c.Payload, &c.Attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, false, nil
	}
	return c, err == nil, err
}
func (s *Store) FinishDelivery(ctx context.Context, c Claim, processErr error) error {
	state, reason := "done", ""
	if processErr != nil {
		state = "pending"
		reason = "processing_retry"
		if errors.Is(processErr, ErrPermanent) {
			state = "dead"
			reason = processErr.Error()
		} else if c.Attempts >= 8 {
			state = "dead"
			reason = "attempt_limit"
		}
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	result, err := tx.Exec(ctx, "UPDATE deliveries SET state=$3,lease_token=NULL,lease_until=NULL,next_attempt=clock_timestamp()+$4::interval,last_error=$5 WHERE id=$1 AND lease_token=$2", c.ID, c.Token, state, RetryDelay(c.Attempts).String(), reason)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	if processErr != nil {
		kind := "delivery_retry"
		if state == "dead" {
			kind = "dead_letter"
		}
		if _, err = tx.Exec(ctx, "INSERT INTO audit(transfer_id,kind,details) VALUES($1,$2,jsonb_build_object('delivery_id',$3::bigint,'reason',$4::text))", c.EventID, kind, c.ID, reason); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
func (s *Store) RetryDead(ctx context.Context, id int64) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	var event uuid.UUID
	err = tx.QueryRow(ctx, "UPDATE deliveries SET state='pending',attempts=0,next_attempt=clock_timestamp(),lease_token=NULL,lease_until=NULL,last_error='' WHERE id=$1 AND state='dead' RETURNING event_id", id).Scan(&event)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("delivery is not dead")
	}
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO audit(transfer_id,kind,details) VALUES($1,'manual_retry',jsonb_build_object('delivery_id',$2::bigint))", event, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) Backlog(ctx context.Context) (map[string]int64, error) {
	out := map[string]int64{}
	for name, q := range map[string]string{"outbox_pending": "SELECT count(*) FROM outbox WHERE published_at IS NULL", "delivery_pending": "SELECT count(*) FROM deliveries WHERE state='pending'", "delivery_dead": "SELECT count(*) FROM deliveries WHERE state='dead'", "transfers_accepted": "SELECT count(*) FROM transfers WHERE state='accepted'"} {
		var n int64
		if err := s.Pool.QueryRow(ctx, q).Scan(&n); err != nil {
			return nil, err
		}
		out[name] = n
	}
	return out, nil
}
