package ledger

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"time"

	"github.com/google/uuid"
)

const MaxAmount int64 = 1_000_000_000_000
const MaxBalance int64 = 1_000_000_000_000_000
const MaxPayload = 4096

var accountID = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)
var keyPattern = regexp.MustCompile(`^[a-zA-Z0-9_:.-]{1,128}$`)
var ErrConflict = errors.New("idempotency key conflicts with existing request")
var ErrInvalid = errors.New("invalid transfer request")
var ErrPermanent = errors.New("permanent event rejection")
var ErrLeaseLost = errors.New("queue lease is no longer owned")

type Request struct {
	From     string `json:"from"`
	To       string `json:"to"`
	Amount   int64  `json:"amount_minor"`
	Currency string `json:"currency"`
}

func (r Request) Validate() error {
	if !accountID.MatchString(r.From) || !accountID.MatchString(r.To) || r.From == r.To {
		return fmt.Errorf("%w: distinct valid accounts required", ErrInvalid)
	}
	if r.Currency != "USD" || r.Amount < 1 || r.Amount > MaxAmount {
		return fmt.Errorf("%w: USD integer amount must be in [1,%d]", ErrInvalid, MaxAmount)
	}
	return nil
}
func (r Request) Hash() []byte { b, _ := json.Marshal(r); h := sha256.Sum256(b); return h[:] }
func ValidateKey(k string) error {
	if !keyPattern.MatchString(k) {
		return fmt.Errorf("%w: Idempotency-Key must be 1-128 letters, digits or _:. - without spaces", ErrInvalid)
	}
	return nil
}

type Event struct {
	Version int       `json:"version"`
	ID      uuid.UUID `json:"event_id"`
	Request Request   `json:"request"`
}

func (e Event) Validate() error {
	if e.Version != 1 || e.ID == uuid.Nil {
		return fmt.Errorf("%w: invalid event identity", ErrPermanent)
	}
	if err := e.Request.Validate(); err != nil {
		return fmt.Errorf("%w: invalid request payload", ErrPermanent)
	}
	return nil
}
func (e Event) Bytes() []byte { b, _ := json.Marshal(e); return b }
func DecodeRequest(b []byte) (Request, error) {
	var r Request
	err := decode(b, &r, []string{"from", "to", "amount_minor", "currency"})
	if err != nil {
		return r, err
	}
	return r, r.Validate()
}
func DecodeEvent(b []byte) (Event, error) {
	var e Event
	if err := decode(b, &e, []string{"version", "event_id", "request"}); err != nil {
		return e, fmt.Errorf("%w: %v", ErrPermanent, err)
	}
	var raw map[string]json.RawMessage
	json.Unmarshal(b, &raw)
	if _, err := DecodeRequest(raw["request"]); err != nil {
		return e, fmt.Errorf("%w: invalid nested request", ErrPermanent)
	}
	return e, e.Validate()
}
func decode(b []byte, dst any, required []string) error {
	if len(b) > MaxPayload {
		return fmt.Errorf("%w: payload exceeds %d bytes", ErrInvalid, MaxPayload)
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return fmt.Errorf("%w: malformed JSON", ErrInvalid)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("%w: one JSON value required", ErrInvalid)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return ErrInvalid
	}
	for _, key := range required {
		v, ok := raw[key]
		if !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			return fmt.Errorf("%w: %s required and non-null", ErrInvalid, key)
		}
	}
	return nil
}
func RetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 8 {
		return 30 * time.Second
	}
	d := 100 * time.Millisecond * time.Duration(1<<attempt)
	if d > 30*time.Second {
		return 30 * time.Second
	}
	return d
}

type Transfer struct {
	ID        uuid.UUID `json:"id"`
	Request   Request   `json:"request"`
	State     string    `json:"state"`
	Reason    string    `json:"reason,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}
type Account struct {
	ID       string `json:"id"`
	Currency string `json:"currency"`
	Balance  int64  `json:"balance_minor"`
	System   bool   `json:"system"`
}
type Claim struct {
	ID       int64
	EventID  uuid.UUID
	Payload  []byte
	Token    uuid.UUID
	Attempts int
}
type Drift struct {
	Account string `json:"account"`
	Cached  int64  `json:"cached_minor"`
	Journal int64  `json:"journal_minor"`
}
type Reconciliation struct {
	Balanced           bool    `json:"balanced"`
	AccountDrift       []Drift `json:"account_drift"`
	UnbalancedJournals int64   `json:"unbalanced_journals"`
	MissingJournals    int64   `json:"posted_without_journal"`
	UnexpectedJournals int64   `json:"journal_without_posted_transfer"`
	MismatchedJournals int64   `json:"journal_request_mismatch"`
	DuplicateJournals  int64   `json:"duplicate_transfer_journals"`
}
