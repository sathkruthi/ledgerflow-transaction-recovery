package ledger

import (
	"bytes"
	"encoding/json"
	"github.com/google/uuid"
	"testing"
	"time"
)

func example() Request { return Request{"alice", "bob", 100, "USD"} }
func TestValidation(t *testing.T) {
	r := example()
	b, _ := json.Marshal(r)
	if got, err := DecodeRequest(b); err != nil || got != r {
		t.Fatal(got, err)
	}
	for _, bad := range []Request{{"alice", "alice", 1, "USD"}, {"alice", "bob", 0, "USD"}, {"alice", "bob", MaxAmount + 1, "USD"}, {"alice", "bob", 1, "EUR"}, {"../alice", "bob", 1, "USD"}} {
		if err := bad.Validate(); err == nil {
			t.Fatal("accepted", bad)
		}
	}
	for _, b := range [][]byte{nil, []byte(`{}`), []byte(`{"from":"alice","to":"bob","amount_minor":null,"currency":"USD"}`), []byte(`{"from":"alice","to":"bob","amount_minor":1.25,"currency":"USD"}`), []byte(`{"from":"alice","to":"bob","amount_minor":1,"currency":"USD","extra":true}`), make([]byte, MaxPayload+1)} {
		if _, err := DecodeRequest(b); err == nil {
			t.Fatalf("invalid JSON accepted: %q", b)
		}
	}
}
func TestIdempotencyHashAndRetry(t *testing.T) {
	r := example()
	same := r
	if !bytes.Equal(r.Hash(), same.Hash()) {
		t.Fatal("unstable hash")
	}
	same.Amount++
	if bytes.Equal(r.Hash(), same.Hash()) {
		t.Fatal("amount omitted from hash")
	}
	for _, key := range []string{"", "space key", string(make([]byte, 129))} {
		if ValidateKey(key) == nil {
			t.Fatal("invalid key", key)
		}
	}
	last := time.Duration(0)
	for a := 0; a < 100; a++ {
		d := RetryDelay(a)
		if d <= 0 || d > 30*time.Second || d < last {
			t.Fatal("unbounded/nonmonotonic retry", a, d)
		}
		last = d
	}
}
func FuzzRequest(f *testing.F) {
	b, _ := json.Marshal(example())
	f.Add(b)
	f.Add([]byte(`{}`))
	f.Fuzz(func(t *testing.T, b []byte) {
		r, err := DecodeRequest(b)
		if err == nil {
			canonical, _ := json.Marshal(r)
			again, err := DecodeRequest(canonical)
			if err != nil || again != r || !bytes.Equal(r.Hash(), again.Hash()) {
				t.Fatal("replay identity changed")
			}
		}
	})
}
func TestEventIdentity(t *testing.T) {
	e := Event{1, uuid.New(), example()}
	decoded, err := DecodeEvent(e.Bytes())
	if err != nil || decoded != e {
		t.Fatal(decoded, err)
	}
	e.ID = uuid.Nil
	if _, err = DecodeEvent(e.Bytes()); err == nil {
		t.Fatal("nil event accepted")
	}
}
