// SPDX-License-Identifier: Apache-2.0

package submit

import (
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/srjn45/rdq/core/envelope"
)

// fixedClock returns a constant time source for deterministic id/timestamps.
func fixedClock(t time.Time) Option {
	return WithClock(func() time.Time { return t })
}

func TestSubmitBuildsPendingEnvelope(t *testing.T) {
	now := time.Date(2026, 7, 20, 14, 3, 22, 117_000_000, time.UTC)
	e, err := Submit("orders", "charge.card", []byte(`{"amount":10}`),
		fixedClock(now),
		WithContentType("application/json"),
		WithHandlerVersion("v2"),
		WithHeader("rdq_source", "kafka://orders/0/42"),
	)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	if e.EnvelopeVersion != envelope.WriteVersion() {
		t.Errorf("envelope_version = %d, want %d", e.EnvelopeVersion, envelope.WriteVersion())
	}
	if e.Status != envelope.StatusPending {
		t.Errorf("status = %q, want PENDING", e.Status)
	}
	if e.Queue != "orders" || e.HandlerRef != "charge.card" || e.HandlerVersion != "v2" {
		t.Errorf("addressing fields wrong: %+v", e)
	}
	if e.PayloadContentType != "application/json" {
		t.Errorf("content type = %q", e.PayloadContentType)
	}
	if e.AttemptCount != 0 || e.RedriveCount != 0 || len(e.Attempts) != 0 {
		t.Errorf("fresh task must have no attempt history: %+v", e)
	}
	if e.LeaseExpiresAt != nil {
		t.Errorf("lease_expires_at must be nil on a fresh task")
	}
	if !e.CreatedAt.Equal(now) {
		t.Errorf("created_at = %v, want %v", e.CreatedAt, now)
	}
	// Due immediately: next_attempt_at == created_at.
	if e.NextAttemptAt == nil || !e.NextAttemptAt.Equal(now) {
		t.Errorf("next_attempt_at = %v, want %v", e.NextAttemptAt, now)
	}
	if _, err := envelope.ParseULID(e.ID); err != nil {
		t.Errorf("id %q is not a valid ULID: %v", e.ID, err)
	}
	// A generated id's timestamp reflects the injected clock.
	id, _ := envelope.ParseULID(e.ID)
	if !id.Time().Equal(now) {
		t.Errorf("generated id timestamp = %v, want %v", id.Time(), now)
	}
	if e.Headers["rdq_source"] != "kafka://orders/0/42" {
		t.Errorf("header not carried: %+v", e.Headers)
	}
}

func TestSubmitDefaultsContentType(t *testing.T) {
	e, err := Submit("orders", "h", []byte("x"))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if e.PayloadContentType != DefaultContentType {
		t.Errorf("content type = %q, want default %q", e.PayloadContentType, DefaultContentType)
	}
}

// TestIdempotentIDReuse is the core acceptance: the same logical submit (same
// queue + idempotency key) yields the same id, so re-submitting is safe.
func TestIdempotentIDReuse(t *testing.T) {
	key := "order-4711-charge"
	// Two independent submits, deliberately at different wall-clock times and
	// with different payloads, must still agree on the id.
	a, err := Submit("orders", "charge.card", []byte("first"),
		WithIdempotencyKey(key),
		fixedClock(time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)),
	)
	if err != nil {
		t.Fatalf("Submit a: %v", err)
	}
	b, err := Submit("orders", "charge.card", []byte("second-try"),
		WithIdempotencyKey(key),
		fixedClock(time.Date(2026, 7, 21, 9, 9, 9, 0, time.UTC)),
	)
	if err != nil {
		t.Fatalf("Submit b: %v", err)
	}
	if a.ID != b.ID {
		t.Fatalf("idempotent submits produced different ids: %q vs %q", a.ID, b.ID)
	}
	if _, err := envelope.ParseULID(a.ID); err != nil {
		t.Errorf("derived id %q is not a valid ULID: %v", a.ID, err)
	}
}

func TestIdempotentIDIsQueueScoped(t *testing.T) {
	key := "same-key"
	a, _ := Submit("orders", "h", nil, WithIdempotencyKey(key))
	b, _ := Submit("payments", "h", nil, WithIdempotencyKey(key))
	if a.ID == b.ID {
		t.Errorf("same key in different queues must yield distinct ids (G8), got %q", a.ID)
	}
}

func TestDifferentKeysDifferentIDs(t *testing.T) {
	a, _ := Submit("orders", "h", nil, WithIdempotencyKey("k1"))
	b, _ := Submit("orders", "h", nil, WithIdempotencyKey("k2"))
	if a.ID == b.ID {
		t.Errorf("distinct keys must yield distinct ids")
	}
}

func TestFreshIDsAreUnique(t *testing.T) {
	// Without an idempotency key, each submit gets its own id even at the same
	// instant (the low 80 bits are random).
	now := time.Date(2026, 7, 20, 14, 3, 22, 0, time.UTC)
	a, _ := Submit("orders", "h", nil, fixedClock(now))
	b, _ := Submit("orders", "h", nil, fixedClock(now))
	if a.ID == b.ID {
		t.Errorf("non-idempotent submits must not collide: %q", a.ID)
	}
}

func TestWithIDExplicit(t *testing.T) {
	// A caller-chosen valid ULID is used verbatim.
	valid := envelope.ULID{}.String() // all-zero bytes -> valid 26-char ULID
	e, err := Submit("orders", "h", nil, WithID(valid))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if e.ID != valid {
		t.Errorf("id = %q, want %q", e.ID, valid)
	}

	if _, err := Submit("orders", "h", nil, WithID("not-a-ulid")); err == nil {
		t.Errorf("WithID with an invalid ULID must error")
	}
}

func TestWithIDAndIdempotencyKeyConflict(t *testing.T) {
	_, err := Submit("orders", "h", nil,
		WithID(envelope.ULID{}.String()),
		WithIdempotencyKey("k"),
	)
	if err == nil {
		t.Errorf("WithID + WithIdempotencyKey must be rejected")
	}
}

func TestSubmitValidatesNames(t *testing.T) {
	cases := []struct {
		name, queue, handler string
	}{
		{"bad queue", "Orders!", "h"},
		{"empty queue", "", "h"},
		{"bad handler", "orders", "Charge Card"},
		{"empty handler", "orders", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := Submit(c.queue, c.handler, nil); err == nil {
				t.Errorf("expected validation error for queue=%q handler=%q", c.queue, c.handler)
			}
		})
	}
}

func TestSubmitRejectsReservedHeaders(t *testing.T) {
	if _, err := Submit("orders", "h", nil, WithHeader("rdq.submitted_by", "me")); err == nil {
		t.Errorf("headers under the reserved rdq. prefix must be rejected")
	}
	if _, err := Submit("orders", "h", nil, WithHeader("", "v")); err == nil {
		t.Errorf("empty header key must be rejected")
	}
}

func TestSubmitCopiesInputs(t *testing.T) {
	payload := []byte("hello")
	headers := map[string]string{"a": "1"}
	e, err := Submit("orders", "h", payload, WithHeaders(headers))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	// Mutating the caller's inputs after the fact must not touch the envelope.
	payload[0] = 'X'
	headers["a"] = "2"
	if string(e.Payload) != "hello" {
		t.Errorf("payload aliased caller buffer: %q", e.Payload)
	}
	if e.Headers["a"] != "1" {
		t.Errorf("headers aliased caller map: %q", e.Headers["a"])
	}
}

// TestBuiltEnvelopeRoundTrips checks the built envelope is well-formed against
// the frozen canonical codec (design 01 §1) — the submit output is exactly what
// storage/server will (de)serialize.
func TestBuiltEnvelopeRoundTrips(t *testing.T) {
	e, err := Submit("orders", "charge.card", []byte(`{"amount":10}`),
		WithIdempotencyKey("k"),
		WithHeader("trace", "abc"),
	)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	b, err := envelope.Marshal(e)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := envelope.Unmarshal(b)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.ID != e.ID || got.Queue != e.Queue || got.Status != e.Status {
		t.Errorf("round-trip changed the envelope: %+v vs %+v", got, e)
	}
}

// TestSubmitDoesNotImportEngine enforces the T4.1 acceptance constraint: the
// submit-only package must be importable without pulling in core/engine (nor any
// storage backend), so a client that only submits does not compile the worker
// runtime into its binary. It asserts against the actual transitive import graph
// rather than trusting the source to stay clean.
func TestSubmitDoesNotImportEngine(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not on PATH; skipping import-graph assertion")
	}
	out, err := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, out)
	}
	forbidden := []string{
		"github.com/srjn45/rdq/core/engine",
		"github.com/srjn45/rdq/storage/postgres",
	}
	for _, dep := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		for _, bad := range forbidden {
			if dep == bad || strings.HasPrefix(dep, bad+"/") {
				t.Errorf("submit must not depend on %q, but it appears in the import graph", dep)
			}
		}
	}
}
