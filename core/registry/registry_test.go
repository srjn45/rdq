// SPDX-License-Identifier: Apache-2.0

package registry_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/memstore"
	"github.com/srjn45/rdq/core/registry"
	"github.com/srjn45/rdq/core/spi"
)

// stubHandler is a Handler with a fixed version that records whether it ran.
type stubHandler struct {
	version string
	err     error
	ran     bool
}

func (h *stubHandler) Version() string { return h.version }

func (h *stubHandler) Handle(ctx context.Context, task envelope.Envelope) error {
	h.ran = true
	return h.err
}

// --- Register / Lookup ---

func TestRegisterAndLookup(t *testing.T) {
	r := registry.New()
	h := &stubHandler{version: "v1"}
	if err := r.Register("h.process", h); err != nil {
		t.Fatalf("Register: %v", err)
	}
	got, ok := r.Lookup("h.process")
	if !ok {
		t.Fatal("Lookup(h.process) = _, false; want registered handler")
	}
	if got != h {
		t.Fatalf("Lookup returned %v, want the registered handler", got)
	}
	if r.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", r.Len())
	}
	if _, ok := r.Lookup("missing"); ok {
		t.Fatal("Lookup(missing) = _, true; want false")
	}
}

func TestRegisterErrors(t *testing.T) {
	r := registry.New()
	if err := r.Register("", &stubHandler{}); !errors.Is(err, registry.ErrEmptyRef) {
		t.Fatalf("Register(empty) = %v, want ErrEmptyRef", err)
	}
	if err := r.Register("h", nil); !errors.Is(err, registry.ErrNilHandler) {
		t.Fatalf("Register(nil) = %v, want ErrNilHandler", err)
	}
	if err := r.Register("h", &stubHandler{version: "v1"}); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	if err := r.Register("h", &stubHandler{version: "v2"}); !errors.Is(err, registry.ErrDuplicateHandler) {
		t.Fatalf("duplicate Register = %v, want ErrDuplicateHandler", err)
	}
	// The duplicate must not have overwritten the original.
	if got, _ := r.Lookup("h"); got.Version() != "v1" {
		t.Fatalf("after rejected duplicate, handler version = %q, want v1", got.Version())
	}
}

func TestPolicyFrom(t *testing.T) {
	cases := []struct {
		in   string
		want registry.Policy
	}{
		{"run-latest", registry.PolicyRunLatest},
		{"dead-letter", registry.PolicyDeadLetter},
		{"", registry.PolicyRunLatest},         // default
		{"nonsense", registry.PolicyRunLatest}, // unknown falls back to default
	}
	for _, c := range cases {
		if got := registry.PolicyFrom(c.in); got != c.want {
			t.Errorf("PolicyFrom(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// --- Resolve (pure) ---

func TestResolve(t *testing.T) {
	newReg := func() *registry.Registry {
		r := registry.New()
		if err := r.Register("h.process", &stubHandler{version: "v2"}); err != nil {
			t.Fatalf("Register: %v", err)
		}
		return r
	}
	task := func(ref, version string) envelope.Envelope {
		return envelope.Envelope{HandlerRef: ref, HandlerVersion: version}
	}

	t.Run("unroutable dead-letters with distinct class", func(t *testing.T) {
		res := newReg().Resolve(task("h.unknown", ""), registry.PolicyRunLatest)
		if res.Action != registry.ActionDeadLetter {
			t.Fatalf("Action = %v, want ActionDeadLetter", res.Action)
		}
		if res.Error == nil || res.Error.Type != registry.ErrorTypeUnroutable {
			t.Fatalf("Error = %+v, want type %q", res.Error, registry.ErrorTypeUnroutable)
		}
		if res.Handler != nil {
			t.Fatal("Handler should be nil on dead-letter")
		}
	})

	t.Run("no version pin runs", func(t *testing.T) {
		res := newReg().Resolve(task("h.process", ""), registry.PolicyDeadLetter)
		if res.Action != registry.ActionRun || res.Handler == nil {
			t.Fatalf("Action=%v Handler=%v, want run with handler", res.Action, res.Handler)
		}
	})

	t.Run("matching version runs", func(t *testing.T) {
		res := newReg().Resolve(task("h.process", "v2"), registry.PolicyDeadLetter)
		if res.Action != registry.ActionRun || res.Handler == nil {
			t.Fatalf("Action=%v Handler=%v, want run with handler", res.Action, res.Handler)
		}
	})

	t.Run("mismatch run-latest ignores pin and runs", func(t *testing.T) {
		res := newReg().Resolve(task("h.process", "v1"), registry.PolicyRunLatest)
		if res.Action != registry.ActionRun || res.Handler == nil {
			t.Fatalf("Action=%v Handler=%v, want run with handler", res.Action, res.Handler)
		}
		if res.Handler.Version() != "v2" {
			t.Fatalf("ran handler version %q, want latest v2", res.Handler.Version())
		}
	})

	t.Run("mismatch dead-letter routes with distinct class", func(t *testing.T) {
		res := newReg().Resolve(task("h.process", "v1"), registry.PolicyDeadLetter)
		if res.Action != registry.ActionDeadLetter {
			t.Fatalf("Action = %v, want ActionDeadLetter", res.Action)
		}
		if res.Error == nil || res.Error.Type != registry.ErrorTypeVersionMismatch {
			t.Fatalf("Error = %+v, want type %q", res.Error, registry.ErrorTypeVersionMismatch)
		}
	})
}

// The two dead-letter classes must be distinct so ops can tell them apart.
func TestErrorClassesDistinct(t *testing.T) {
	if registry.ErrorTypeUnroutable == registry.ErrorTypeVersionMismatch {
		t.Fatal("Unroutable and VersionMismatch error classes must be distinct")
	}
}

// --- Integration against memstore (ACCEPTANCE) ---

// testClock is a manually advanced clock; the store is the time authority (G9).
type testClock struct{ now time.Time }

func (c *testClock) Now() time.Time          { return c.now }
func (c *testClock) advance(d time.Duration) { c.now = c.now.Add(d) }

func newStore() (*memstore.Store, *testClock) {
	clk := &testClock{now: time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)}
	return memstore.New(memstore.WithClock(clk.Now)), clk
}

func pendingTask(id, queue, ref, version string, dueAt time.Time) envelope.Envelope {
	return envelope.Envelope{
		EnvelopeVersion:    1,
		ID:                 id,
		Queue:              queue,
		HandlerRef:         ref,
		HandlerVersion:     version,
		Payload:            []byte("p"),
		PayloadContentType: "application/octet-stream",
		Status:             envelope.StatusPending,
		NextAttemptAt:      &dueAt,
		CreatedAt:          dueAt,
	}
}

// routeToDLQ performs the worker's dead-letter step for a resolution: it wraps
// the resolution's Error into a PERMANENT_FAILURE attempt and calls DeadLetter,
// exactly as the worker runtime (T3.6) will. Returns after the store move.
func routeToDLQ(t *testing.T, s *memstore.Store, c spi.Claimed, res registry.Resolution, at time.Time) {
	t.Helper()
	fin := at
	att := envelope.Attempt{
		AttemptNo:  c.Task.AttemptCount + 1,
		StartedAt:  at,
		FinishedAt: &fin,
		Outcome:    envelope.OutcomePermanentFailure,
		Error:      res.Error,
	}
	if err := s.DeadLetter(context.Background(), c.Task.ID, c.Token, att); err != nil {
		t.Fatalf("DeadLetter: %v", err)
	}
}

// assertDeadWithError verifies the task is DEAD, records the expected error
// class on its final attempt, and — the hot-loop invariant — is never due
// again no matter how far the clock advances.
func assertDeadWithError(t *testing.T, s *memstore.Store, clk *testClock, queue, id, wantErrType string) {
	t.Helper()
	got, err := s.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != envelope.StatusDead {
		t.Fatalf("status = %q, want DEAD", got.Status)
	}
	if n := len(got.Attempts); n == 0 {
		t.Fatal("dead task has no attempts")
	} else if last := got.Attempts[n-1]; last.Error == nil || last.Error.Type != wantErrType {
		t.Fatalf("final attempt error = %+v, want type %q", last.Error, wantErrType)
	}

	// Never hot-loops: a DEAD task is not due, even far in the future.
	clk.advance(365 * 24 * time.Hour)
	claimed, err := s.ClaimDue(context.Background(), queue, 100, time.Minute)
	if err != nil {
		t.Fatalf("ClaimDue after dead-letter: %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("ClaimDue returned %d tasks after dead-letter; a dead-lettered task must never be re-claimed (hot-loop)", len(claimed))
	}
}

func TestUnroutableRoutesToDLQ(t *testing.T) {
	s, clk := newStore()
	ctx := context.Background()
	const queue = "payments.charge"

	// No handler is registered for this ref.
	reg := registry.New()

	task := pendingTask("t-unroutable", queue, "h.gone", "", clk.now)
	if err := s.Enqueue(ctx, task); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	claimed, err := s.ClaimDue(ctx, queue, 10, time.Minute)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("ClaimDue returned %d, want 1", len(claimed))
	}

	res := reg.Resolve(claimed[0].Task, registry.PolicyRunLatest)
	if res.Action != registry.ActionDeadLetter {
		t.Fatalf("Action = %v, want ActionDeadLetter", res.Action)
	}
	routeToDLQ(t, s, claimed[0], res, clk.now)

	assertDeadWithError(t, s, clk, queue, "t-unroutable", registry.ErrorTypeUnroutable)

	// And it is visible in the DLQ under its distinct class.
	dead, _, err := s.DLQList(ctx, queue, spi.DLQFilter{ErrorType: registry.ErrorTypeUnroutable, IncludeAttempts: true}, spi.Page{})
	if err != nil {
		t.Fatalf("DLQList: %v", err)
	}
	if len(dead) != 1 || dead[0].ID != "t-unroutable" {
		t.Fatalf("DLQList = %v, want the unroutable task", dead)
	}
}

func TestVersionMismatchDeadLetterRoutesToDLQ(t *testing.T) {
	s, clk := newStore()
	ctx := context.Background()
	const queue = "payments.charge"

	reg := registry.New()
	if err := reg.Register("h.charge", &stubHandler{version: "v3"}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Task pins v1 but the worker only has v3, under the strict policy.
	task := pendingTask("t-mismatch", queue, "h.charge", "v1", clk.now)
	if err := s.Enqueue(ctx, task); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	claimed, err := s.ClaimDue(ctx, queue, 10, time.Minute)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("ClaimDue returned %d, want 1", len(claimed))
	}

	res := reg.Resolve(claimed[0].Task, registry.PolicyDeadLetter)
	if res.Action != registry.ActionDeadLetter {
		t.Fatalf("Action = %v, want ActionDeadLetter", res.Action)
	}
	routeToDLQ(t, s, claimed[0], res, clk.now)

	assertDeadWithError(t, s, clk, queue, "t-mismatch", registry.ErrorTypeVersionMismatch)
}

// Under run-latest a mismatched task is invoked, not dead-lettered: the handler
// runs and the task completes.
func TestVersionMismatchRunLatestInvokesHandler(t *testing.T) {
	s, clk := newStore()
	ctx := context.Background()
	const queue = "payments.charge"

	reg := registry.New()
	h := &stubHandler{version: "v3"}
	if err := reg.Register("h.charge", h); err != nil {
		t.Fatalf("Register: %v", err)
	}

	task := pendingTask("t-runlatest", queue, "h.charge", "v1", clk.now)
	if err := s.Enqueue(ctx, task); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	claimed := mustClaimOne(t, s, queue)

	res := reg.Resolve(claimed.Task, registry.PolicyRunLatest)
	if res.Action != registry.ActionRun || res.Handler == nil {
		t.Fatalf("Action=%v Handler=%v, want run with handler", res.Action, res.Handler)
	}
	if err := res.Handler.Handle(ctx, claimed.Task); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if !h.ran {
		t.Fatal("handler did not run under run-latest")
	}

	// Complete the task as the worker would on handler success.
	fin := clk.now
	att := envelope.Attempt{AttemptNo: 1, StartedAt: clk.now, FinishedAt: &fin, Outcome: envelope.OutcomeSuccess}
	if err := s.Complete(ctx, claimed.Task.ID, claimed.Token, att); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	got, err := s.Get(ctx, "t-runlatest")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != envelope.StatusSucceeded {
		t.Fatalf("status = %q, want SUCCEEDED", got.Status)
	}
}

func mustClaimOne(t *testing.T, s *memstore.Store, queue string) spi.Claimed {
	t.Helper()
	claimed, err := s.ClaimDue(context.Background(), queue, 10, time.Minute)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("ClaimDue returned %d, want 1", len(claimed))
	}
	return claimed[0]
}
