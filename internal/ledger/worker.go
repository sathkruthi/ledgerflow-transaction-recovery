package ledger

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
)

// PublishOne deliberately keeps the relay transaction separate from outbox acknowledgement.
// afterSend is only used by an explicitly enabled local process-crash demonstration.
func (s *Store) PublishOne(ctx context.Context, client *http.Client, relayURL string, lease time.Duration, afterSend func()) (bool, error) {
	c, ok, err := s.ClaimOutbox(ctx, lease)
	if err != nil || !ok {
		return false, err
	}
	ctx, span := otel.Tracer("ledgerflow/publisher").Start(ctx, "outbox.publish")
	defer span.End()
	span.SetAttributes(attribute.String("event.id", c.EventID.String()), attribute.Int("attempt", c.Attempts))
	op, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(op, http.MethodPost, relayURL+"/events", bytes.NewReader(c.Payload))
	if err != nil {
		return true, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Correlation-ID", c.EventID.String())
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	response, sendErr := client.Do(req)
	success := false
	if sendErr == nil {
		success = response.StatusCode == http.StatusCreated
		io.Copy(io.Discard, io.LimitReader(response.Body, 1024))
		response.Body.Close()
	}
	if afterSend != nil && success {
		afterSend()
	}
	if !success {
		span.SetStatus(codes.Error, "relay acknowledgement unavailable")
	}
	// Use caller context: if cancelled after relay receipt, the lease expires and publication retries.
	if err = s.FinishOutbox(ctx, c, success); err != nil {
		return true, err
	}
	if !success {
		return true, fmt.Errorf("relay acknowledgement unavailable")
	}
	slog.Info("outbox acknowledged", "event_id", c.EventID, "correlation_id", c.EventID, "attempt", c.Attempts)
	return true, nil
}
func (s *Store) ConsumeOne(ctx context.Context, lease time.Duration) (bool, string, error) {
	c, ok, err := s.ClaimDelivery(ctx, lease)
	if err != nil || !ok {
		return false, "", err
	}
	ctx, span := otel.Tracer("ledgerflow/consumer").Start(ctx, "ledger.consume")
	defer span.End()
	span.SetAttributes(attribute.String("event.id", c.EventID.String()), attribute.Int64("delivery.id", c.ID))
	e, processErr := DecodeEvent(c.Payload)
	outcome := ""
	if processErr == nil {
		if e.ID != c.EventID {
			processErr = fmt.Errorf("%w: delivery identity mismatch", ErrPermanent)
		} else {
			op, cancel := context.WithTimeout(ctx, 4*time.Second)
			outcome, processErr = s.Process(op, e)
			cancel()
		}
	}
	if processErr != nil {
		span.SetStatus(codes.Error, "processing failed")
	}
	if err = s.FinishDelivery(ctx, c, processErr); err != nil {
		return true, outcome, err
	}
	slog.Info("delivery processed", "event_id", c.EventID, "delivery_id", c.ID, "correlation_id", c.EventID, "outcome", outcome, "attempt", c.Attempts)
	return true, outcome, processErr
}
func (s *Store) RunWorker(ctx context.Context, mode, relay string, lease time.Duration, crash func(), log *slog.Logger) {
	client := &http.Client{Timeout: 3 * time.Second}
	for ctx.Err() == nil {
		op, cancel := context.WithTimeout(ctx, 7*time.Second)
		var worked bool
		var err error
		outcome := ""
		if mode == "publisher" {
			worked, err = s.PublishOne(op, client, relay, lease, crash)
		} else {
			worked, outcome, err = s.ConsumeOne(op, lease)
		}
		cancel()
		if err != nil {
			log.Warn("worker attempt failed", "mode", mode, "reason", "see durable queue state")
		} else if worked {
			log.Info("worker committed", "mode", mode, "outcome", outcome)
		}
		if worked && err == nil {
			continue
		}
		delay := 50 * time.Millisecond
		if err != nil {
			delay = 250 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
