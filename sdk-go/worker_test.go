// SPDX-License-Identifier: Apache-2.0

package rdq_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/memstore"
	"github.com/srjn45/rdq/core/policy"
	"github.com/srjn45/rdq/core/registry"
	rdq "github.com/srjn45/rdq/sdk-go"
	"github.com/srjn45/rdq/sdk-go/submit"
)

// Handler names used across the memstore test suite. Each name is unique so
// registrations into the process-level defaultReg do not collide.
const (
	hdlMSOK    = "rdq.t.ms.ok"
	hdlMSPerm  = "rdq.t.ms.perm"
	hdlMSRetry = "rdq.t.ms.retry"
	hdlMSMap   = "rdq.t.ms.map"
	hdlDup     = "rdq.t.dup"
)

// mustRegister registers fn under name, tolerating ErrDuplicateHandler for
// repeated test runs (e.g. go test -count=2) where the process-level registry
// already holds the handler from the first pass.
func mustRegister(t *testing.T, name string, fn rdq.HandlerFunc) {
	t.Helper()
	if err := rdq.Register(name, fn); err != nil && !errors.Is(err, registry.ErrDuplicateHandler) {
		t.Fatalf("Register(%q): %v", name, err)
	}
}

// msSpec returns a fast QueueSpec suitable for memstore tests: short lease,
// aggressive poll, simple backoff ladder.
func msSpec(queue string) rdq.QueueSpec {
	return rdq.QueueSpec{
		Queue:          queue,
		MaxAttempts:    3,
		Backoff:        policy.Backoff{Initial: 50 * time.Millisecond, Multiplier: 1, Max: time.Hour},
		Classifier:     policy.Classifier{},
		Lease:          500 * time.Millisecond,
		HandlerTimeout: 400 * time.Millisecond,
		BatchSize:      8,
		Concurrency:    4,
		PollInterval:   5 * time.Millisecond,
	}
}

// enqueue submits a task to store and returns the task ID.
func enqueue(t *testing.T, store *memstore.Store, queue, handlerRef string) string {
	t.Helper()
	env, err := submit.Submit(queue, handlerRef, []byte("{}"))
	if err != nil {
		t.Fatalf("submit.Submit: %v", err)
	}
	if err := store.Enqueue(context.Background(), *env); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	return env.ID
}

// waitTerminal polls store until the task with id reaches SUCCEEDED or DEAD,
// then returns the final envelope. Fails the test after 10 s.
func waitTerminal(t *testing.T, store *memstore.Store, id string) envelope.Envelope {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		env, err := store.Get(context.Background(), id)
		if err != nil {
			t.Fatalf("Get(%s): %v", id, err)
		}
		if env.Status == envelope.StatusSucceeded || env.Status == envelope.StatusDead {
			return env
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("task %s did not reach terminal state within 10s", id)
	panic("unreachable")
}

// runWorker starts w in the background and returns a cancel func. Cancel when
// the task under test has reached a terminal state.
func runWorker(t *testing.T, w *rdq.Worker) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = w.Run(ctx) }()
	t.Cleanup(cancel)
	return cancel
}

// --- registration contract tests ---

func TestRegister_ErrDuplicateHandler(t *testing.T) {
	_ = rdq.Register(hdlDup, func(_ context.Context, _ envelope.Envelope) error { return nil })
	err := rdq.Register(hdlDup, func(_ context.Context, _ envelope.Envelope) error { return nil })
	if !errors.Is(err, registry.ErrDuplicateHandler) {
		t.Fatalf("want ErrDuplicateHandler, got %v", err)
	}
}

func TestRegister_ErrNilHandler(t *testing.T) {
	if err := rdq.Register("rdq.t.nil", nil); !errors.Is(err, registry.ErrNilHandler) {
		t.Fatalf("want ErrNilHandler, got %v", err)
	}
}

func TestRegister_ErrEmptyRef(t *testing.T) {
	fn := func(_ context.Context, _ envelope.Envelope) error { return nil }
	if err := rdq.Register("", fn); !errors.Is(err, registry.ErrEmptyRef) {
		t.Fatalf("want ErrEmptyRef, got %v", err)
	}
}

// --- memstore worker behaviour tests ---

// TestWorker_Memstore_Success verifies the happy path: a handler that returns
// nil drives the task to SUCCEEDED.
func TestWorker_Memstore_Success(t *testing.T) {
	mustRegister(t, hdlMSOK, func(_ context.Context, _ envelope.Envelope) error { return nil })

	store := memstore.New()
	id := enqueue(t, store, "ms.ok", hdlMSOK)

	w, err := rdq.NewWorker(store, []rdq.QueueSpec{msSpec("ms.ok")})
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	cancel := runWorker(t, w)

	env := waitTerminal(t, store, id)
	cancel()
	if env.Status != envelope.StatusSucceeded {
		t.Fatalf("status = %s, want SUCCEEDED", env.Status)
	}
}

// TestWorker_Memstore_Permanent verifies that rdq.Permanent(err) forces the
// task to the DLQ on the first attempt, without retry.
func TestWorker_Memstore_Permanent(t *testing.T) {
	mustRegister(t, hdlMSPerm, func(_ context.Context, _ envelope.Envelope) error {
		return rdq.Permanent(errors.New("not retryable"))
	})

	store := memstore.New()
	id := enqueue(t, store, "ms.perm", hdlMSPerm)

	w, err := rdq.NewWorker(store, []rdq.QueueSpec{msSpec("ms.perm")})
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	cancel := runWorker(t, w)

	env := waitTerminal(t, store, id)
	cancel()
	if env.Status != envelope.StatusDead {
		t.Fatalf("status = %s, want DEAD", env.Status)
	}
	if env.AttemptCount != 1 {
		t.Fatalf("attempt_count = %d, want 1 (no retry after Permanent)", env.AttemptCount)
	}
}

// TestWorker_Memstore_Retryable verifies that rdq.Retryable(err) reschedules
// the task, and a subsequent nil return drives it to SUCCEEDED.
func TestWorker_Memstore_Retryable(t *testing.T) {
	var calls atomic.Int32
	mustRegister(t, hdlMSRetry, func(_ context.Context, _ envelope.Envelope) error {
		if calls.Add(1) == 1 {
			return rdq.Retryable(errors.New("transient"))
		}
		return nil
	})

	store := memstore.New()
	id := enqueue(t, store, "ms.retry", hdlMSRetry)

	spec := msSpec("ms.retry")
	spec.Backoff = policy.Backoff{Initial: 10 * time.Millisecond, Multiplier: 1, Max: time.Hour}

	w, err := rdq.NewWorker(store, []rdq.QueueSpec{spec})
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	cancel := runWorker(t, w)

	env := waitTerminal(t, store, id)
	cancel()
	if env.Status != envelope.StatusSucceeded {
		t.Fatalf("status = %s, want SUCCEEDED after retry", env.Status)
	}
	if n := int(calls.Load()); n != 2 {
		t.Fatalf("handler called %d times, want 2", n)
	}
}

// TestWorker_Memstore_OutcomeMapper verifies that an OutcomeMapper in
// QueueSpec.Classifier.Mapper takes authoritative precedence (layer 1).
// The handler returns a plain error, but the mapper classifies it as
// Permanent, dead-lettering the task on the first attempt.
func TestWorker_Memstore_OutcomeMapper(t *testing.T) {
	sentinel := errors.New("my-sentinel")
	mustRegister(t, hdlMSMap, func(_ context.Context, _ envelope.Envelope) error {
		return sentinel
	})

	store := memstore.New()
	id := enqueue(t, store, "ms.map", hdlMSMap)

	spec := msSpec("ms.map")
	spec.Classifier = policy.Classifier{
		Mapper: func(err error) (rdq.Decision, bool) {
			if errors.Is(err, sentinel) {
				return rdq.DecisionPermanent, true
			}
			return 0, false
		},
	}

	w, err := rdq.NewWorker(store, []rdq.QueueSpec{spec})
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	cancel := runWorker(t, w)

	env := waitTerminal(t, store, id)
	cancel()
	if env.Status != envelope.StatusDead {
		t.Fatalf("status = %s, want DEAD (OutcomeMapper said Permanent)", env.Status)
	}
	if env.AttemptCount != 1 {
		t.Fatalf("attempt_count = %d, want 1 (mapper short-circuits retries)", env.AttemptCount)
	}
}
