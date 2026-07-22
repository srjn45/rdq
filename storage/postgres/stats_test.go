// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/spi"
)

// liveEnvelope builds an attempt-less task in the given non-DEAD status.
func liveEnvelope(id, queue string, status envelope.Status, createdAt time.Time) envelope.Envelope {
	return envelope.Envelope{
		EnvelopeVersion:    1,
		ID:                 id,
		Queue:              queue,
		HandlerRef:         "h",
		Payload:            []byte(`{}`),
		PayloadContentType: "application/json",
		Status:             status,
		CreatedAt:          createdAt,
	}
}

// succeededEnvelope builds a SUCCEEDED task whose single SUCCESS attempt finished
// at finishedAt — the completion instant PurgeSucceeded ages against.
func succeededEnvelope(id, queue string, finishedAt time.Time) envelope.Envelope {
	fin := finishedAt
	return envelope.Envelope{
		EnvelopeVersion:    1,
		ID:                 id,
		Queue:              queue,
		HandlerRef:         "h",
		Payload:            []byte(`{}`),
		PayloadContentType: "application/json",
		Status:             envelope.StatusSucceeded,
		AttemptCount:       1,
		Attempts: []envelope.Attempt{
			{AttemptNo: 1, StartedAt: finishedAt.Add(-time.Second), FinishedAt: &fin, Outcome: envelope.OutcomeSuccess},
		},
		CreatedAt: baseTime.Add(-2 * time.Hour),
	}
}

// TestStats checks the per-queue snapshot: PENDING/IN_FLIGHT counts, DLQ depth,
// and OldestPendingAge (oldest PENDING by created_at, against the backend clock).
func TestStats(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(ctx, t)
	const q = "q.stats"

	// Two PENDING with distinct ages; Stats must age against the OLDER one.
	old := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	newer := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	seedLive(ctx, t, s, liveEnvelope("p_old", q, envelope.StatusPending, old))
	seedLive(ctx, t, s, liveEnvelope("p_new", q, envelope.StatusPending, newer))
	seedLive(ctx, t, s, liveEnvelope("inflight", q, envelope.StatusInFlight, baseTime))
	seedLive(ctx, t, s, liveEnvelope("done", q, envelope.StatusSucceeded, baseTime))
	seedDead(ctx, t, s, deadEnvelope("dead", q, "h", "boom"), baseTime)
	// Noise in another queue must not leak in.
	seedLive(ctx, t, s, liveEnvelope("other", "q.elsewhere", envelope.StatusPending, old))

	st, err := s.Stats(ctx, q)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if st.Pending != 2 {
		t.Errorf("Pending = %d, want 2", st.Pending)
	}
	if st.InFlight != 1 {
		t.Errorf("InFlight = %d, want 1", st.InFlight)
	}
	if st.DLQDepth != 1 {
		t.Errorf("DLQDepth = %d, want 1", st.DLQDepth)
	}
	// Oldest pending is 2020-01-01; today is 2026-07-21, so the age exceeds six
	// years — which also proves the MIN (older) created_at was chosen, not 2021.
	if st.OldestPendingAge < 6*365*24*time.Hour {
		t.Errorf("OldestPendingAge = %v, want > 6y (oldest pending is 2020)", st.OldestPendingAge)
	}

	// A queue with no pending work reports zero age and empty counts.
	empty, err := s.Stats(ctx, "q.void")
	if err != nil {
		t.Fatalf("Stats empty: %v", err)
	}
	if empty != (spi.Stats{}) {
		t.Errorf("Stats(empty queue) = %+v, want zero value", empty)
	}
}

// TestPurgeSucceeded checks task_ttl enforcement: only SUCCEEDED tasks completed
// before olderThan are removed (with their attempt rows); the age basis is the
// terminal attempt's finished_at, falling back to created_at when a success has
// no attempts. Non-SUCCEEDED tasks are never touched.
func TestPurgeSucceeded(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(ctx, t)
	const q = "q.ttl"

	seedLive(ctx, t, s, succeededEnvelope("s_old", q, baseTime))                // finished baseTime
	seedLive(ctx, t, s, succeededEnvelope("s_new", q, baseTime.Add(time.Hour))) // finished +1h
	// SUCCEEDED with no attempts → age falls back to created_at (well in the past).
	seedLive(ctx, t, s, liveEnvelope("s_noatt", q, envelope.StatusSucceeded, baseTime.Add(-time.Hour)))
	// A PENDING task must survive regardless of age.
	seedLive(ctx, t, s, liveEnvelope("pending", q, envelope.StatusPending, baseTime.Add(-time.Hour)))

	olderThan := baseTime.Add(30 * time.Minute)
	n, err := s.PurgeSucceeded(ctx, q, olderThan)
	if err != nil {
		t.Fatalf("PurgeSucceeded: %v", err)
	}
	if n != 2 {
		t.Fatalf("PurgeSucceeded count = %d, want 2 (s_old, s_noatt)", n)
	}

	if _, err := s.Get(ctx, "s_old"); !errors.Is(err, spi.ErrNotFound) {
		t.Errorf("s_old survived purge: %v", err)
	}
	if c := countAttempts(ctx, t, s, "s_old"); c != 0 {
		t.Errorf("s_old attempt rows after purge = %d, want 0", c)
	}
	if _, err := s.Get(ctx, "s_noatt"); !errors.Is(err, spi.ErrNotFound) {
		t.Errorf("s_noatt survived purge: %v", err)
	}
	if g, err := s.Get(ctx, "s_new"); err != nil || g.Status != envelope.StatusSucceeded {
		t.Errorf("s_new = (%v, %v), want SUCCEEDED (too recent to purge)", g.Status, err)
	}
	if g, err := s.Get(ctx, "pending"); err != nil || g.Status != envelope.StatusPending {
		t.Errorf("pending task = (%v, %v), want PENDING (never purged)", g.Status, err)
	}
}
