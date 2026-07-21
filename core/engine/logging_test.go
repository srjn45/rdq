// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/srjn45/rdq/core/envelope"
	rdqlog "github.com/srjn45/rdq/core/log"
	"github.com/srjn45/rdq/core/memstore"
	"github.com/srjn45/rdq/core/registry"
)

// syncBuf is a concurrency-safe io.Writer: transition logs are emitted from
// handler goroutines, so the test sink must guard its buffer.
type syncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// records decodes the buffer's JSON lines into maps.
func (s *syncBuf) records(t *testing.T) []map[string]any {
	t.Helper()
	var recs []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(s.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line is not JSON: %v\n%s", err, line)
		}
		recs = append(recs, m)
	}
	return recs
}

const testTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

// submitWithHeaders enqueues a PENDING task carrying the given payload + headers.
func submitWithHeaders(t *testing.T, store *memstore.Store, clk *workerClock, id string, payload []byte, headers map[string]string) {
	t.Helper()
	now := clk.Now()
	err := store.Enqueue(context.Background(), envelope.Envelope{
		EnvelopeVersion:    1,
		ID:                 id,
		Queue:              testQueue,
		HandlerRef:         "h",
		Payload:            payload,
		PayloadContentType: "application/json",
		Headers:            headers,
		Status:             envelope.StatusPending,
		NextAttemptAt:      &now,
		CreatedAt:          now,
	})
	if err != nil {
		t.Fatalf("enqueue %s: %v", id, err)
	}
}

// TestTransitionLogging_HappyPath proves the engine emits structured transition
// records (claimed → succeeded) carrying task id + queue + trace_id, propagates
// the traceparent into the handler's context, and never logs the payload bytes.
func TestTransitionLogging_HappyPath(t *testing.T) {
	clk := newWorkerClock()
	store := memstore.New(memstore.WithClock(clk.Now))
	reg := registry.New()

	var handlerTP string
	var mu sync.Mutex
	if err := reg.Register("h", &testHandler{fn: func(ctx context.Context, task envelope.Envelope) error {
		mu.Lock()
		handlerTP = rdqlog.TraceparentFromContext(ctx)
		mu.Unlock()
		return nil
	}}); err != nil {
		t.Fatal(err)
	}

	var sink syncBuf
	w := newTestWorker(t, store, reg, clk, []QueueSpec{baseSpec()}, WithLogger(rdqlog.New(&sink)))
	cancel, done := runWorker(t, w)
	defer func() { cancel(); <-done }()

	secret := []byte(`{"pan":"4111111111111111","secret":"do-not-log-me"}`)
	submitWithHeaders(t, store, clk, "t1", secret, map[string]string{
		rdqlog.HeaderTraceparent: testTraceparent,
	})

	eventually(t, func() bool { return getTask(t, store, "t1").Status == envelope.StatusSucceeded })
	// Wait until both transition records have landed.
	eventually(t, func() bool {
		recs := sink.records(t)
		var claimed, succeeded bool
		for _, r := range recs {
			switch r[rdqlog.KeyTransition] {
			case string(rdqlog.TransitionClaimed):
				claimed = true
			case string(rdqlog.TransitionSucceeded):
				succeeded = true
			}
		}
		return claimed && succeeded
	})

	// The handler saw the propagated traceparent (submit → handler).
	mu.Lock()
	gotTP := handlerTP
	mu.Unlock()
	if gotTP != testTraceparent {
		t.Errorf("handler traceparent = %q, want %q", gotTP, testTraceparent)
	}

	// Every transition record carries id + queue; the trace_id is the parsed
	// trace id; and the raw payload never appears anywhere in the log.
	const wantTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	for _, r := range sink.records(t) {
		if r[rdqlog.KeyTaskID] != "t1" {
			t.Errorf("record %v missing task_id t1", r)
		}
		if r[rdqlog.KeyQueue] != testQueue {
			t.Errorf("record %v queue = %v, want %s", r, r[rdqlog.KeyQueue], testQueue)
		}
		if r[rdqlog.KeyTraceID] != wantTraceID {
			t.Errorf("record %v trace_id = %v, want %s", r, r[rdqlog.KeyTraceID], wantTraceID)
		}
	}

	out := sink.String()
	for _, needle := range []string{"4111111111111111", "do-not-log-me"} {
		if strings.Contains(out, needle) {
			t.Fatalf("transition log leaked payload %q:\n%s", needle, out)
		}
	}
}

// TestTransitionLogging_DeadLetter proves a permanent failure emits a
// dead_lettered record with the error type, still redacting the payload.
func TestTransitionLogging_DeadLetter(t *testing.T) {
	clk := newWorkerClock()
	store := memstore.New(memstore.WithClock(clk.Now))
	reg := registry.New()

	// Unroutable handler_ref → routing dead-letter on first claim.
	if err := reg.Register("other", &testHandler{fn: func(context.Context, envelope.Envelope) error {
		return nil
	}}); err != nil {
		t.Fatal(err)
	}

	var sink syncBuf
	w := newTestWorker(t, store, reg, clk, []QueueSpec{baseSpec()}, WithLogger(rdqlog.New(&sink)))
	cancel, done := runWorker(t, w)
	defer func() { cancel(); <-done }()

	submitWithHeaders(t, store, clk, "dead1", []byte(`{"card":"5555444433332222"}`), nil)

	eventually(t, func() bool { return getTask(t, store, "dead1").Status == envelope.StatusDead })
	eventually(t, func() bool {
		for _, r := range sink.records(t) {
			if r[rdqlog.KeyTransition] == string(rdqlog.TransitionDeadLettered) {
				return true
			}
		}
		return false
	})

	var found map[string]any
	for _, r := range sink.records(t) {
		if r[rdqlog.KeyTransition] == string(rdqlog.TransitionDeadLettered) {
			found = r
		}
	}
	if found == nil {
		t.Fatal("no dead_lettered record")
	}
	if found[rdqlog.KeyErrorType] == nil || found[rdqlog.KeyErrorType] == "" {
		t.Errorf("dead_lettered record missing error_type: %v", found)
	}
	if strings.Contains(sink.String(), "5555444433332222") {
		t.Fatalf("dead-letter log leaked payload:\n%s", sink.String())
	}
}
