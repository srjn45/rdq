// SPDX-License-Identifier: Apache-2.0

// Package spi is the storage service-provider interface: the Storage contract
// every backend (the in-memory reference store, Postgres, and third-party
// plugins) implements, plus its value and error types (design 02 §2). It is a
// FROZEN contract — the compliance kit verifies backends against the
// correctness invariants in design 02 §3.
//
// Time authority. The storage backend's clock is the single source of truth for
// due-ness and lease expiry (G9): the engine computes next_attempt_at values,
// but the backend decides "now" when evaluating them (e.g. Postgres now()).
// Engines tolerate clock skew accordingly.
//
// Atomicity & fencing. Every mutating method is all-or-nothing (design 02 §3):
// a crash between any two calls leaves a task in a valid state — at worst
// retried after lease expiry (at-least-once, never lost). Claims are fenced by
// ClaimToken: at most one valid token per task exists at any moment, and any
// outcome call bearing a stale token fails with ErrStaleClaim and changes
// nothing.
package spi

import (
	"context"
	"time"

	"github.com/srjn45/rdq/core/envelope"
)

// Storage is the mandatory floor every backend implements. Method
// documentation restates the invariants the compliance kit tests (design 02
// §3); optional accelerations are advertised via Capabilities.
type Storage interface {
	// --- lifecycle ---

	// Enqueue admits a task. Idempotent by task.ID within a queue: re-enqueue
	// of an existing id in the SAME queue is a no-op (safe submit retries). The
	// same id already present in a DIFFERENT queue is NOT a no-op — it returns
	// ErrIDConflict, since a silent cross-queue no-op would return a confusing
	// foreign envelope (G8). Maps to HTTP 409 at the API.
	Enqueue(ctx context.Context, task envelope.Envelope) error

	// ClaimDue atomically claims up to limit due tasks for queue. A task is due
	// when, by the backend's clock (G9):
	//   (status=PENDING   AND next_attempt_at   <= now)
	//   OR (status=IN_FLIGHT AND lease_expires_at <= now)   // crash recovery
	// Claimed tasks become IN_FLIGHT with lease_expires_at = now + lease.
	// Reclaiming an expired lease atomically appends a LEASE_EXPIRED attempt
	// record. Ordering is best-effort by next_attempt_at ascending. It NEVER
	// returns a task another live claim holds, and mints one fencing ClaimToken
	// per returned task.
	ClaimDue(ctx context.Context, queue string, limit int, lease time.Duration) ([]Claimed, error)

	// ExtendLease renews the lease for a long-running handler. It fails with
	// ErrStaleClaim if the lease was lost (task reclaimed elsewhere), on which
	// the handler must abandon its work.
	ExtendLease(ctx context.Context, id TaskID, token ClaimToken, lease time.Duration) error

	// --- outcome resolution (all require a valid token; ErrStaleClaim otherwise) ---

	// Reschedule is the failure path: append attempt, set the task PENDING with
	// next_attempt_at = nextAt (engine-computed backoff).
	Reschedule(ctx context.Context, id TaskID, token ClaimToken, attempt Attempt, nextAt time.Time) error

	// Complete is the success path: append attempt and mark the task SUCCEEDED
	// (retained until task_ttl purge).
	Complete(ctx context.Context, id TaskID, token ClaimToken, attempt Attempt) error

	// DeadLetter is exhaustion / permanent failure: append attempt and move the
	// task to the DLQ.
	DeadLetter(ctx context.Context, id TaskID, token ClaimToken, attempt Attempt) error

	// --- DLQ ---

	// DLQList pages the dead-letter queue for queue with stable cursor-based
	// pagination (no skips/dupes across pages while entries are added, design
	// 02 §3). Envelopes are returned WITHOUT attempt bodies unless
	// f.IncludeAttempts is set — histories make pages heavy (G13). The returned
	// Cursor is empty on the last page; an unresolvable cursor yields
	// ErrStaleCursor.
	DLQList(ctx context.Context, queue string, f DLQFilter, page Page) ([]envelope.Envelope, Cursor, error)

	// Get fetches one task by id in ANY status (PENDING/IN_FLIGHT/SUCCEEDED/
	// DEAD) with full attempt history; ErrNotFound if absent. Backs
	// GET /v1/tasks/{id}; replaces the DEAD-only DLQGet (G4).
	Get(ctx context.Context, id TaskID) (envelope.Envelope, error)

	// Redrive returns the selected DLQ tasks to PENDING with attempt_count=0
	// and redrive_count incremented, preserving prior attempt history (envelope
	// §2). Returns the count affected.
	Redrive(ctx context.Context, queue string, sel Selector) (int, error)

	// Purge permanently removes the selected DLQ tasks. Returns the count
	// removed.
	Purge(ctx context.Context, queue string, sel Selector) (int, error)

	// --- ops ---

	// Stats returns a per-queue operational snapshot (pending, in_flight,
	// dlq_depth, oldest_pending_age) powering the Prometheus metrics.
	Stats(ctx context.Context, queue string) (Stats, error)

	// PurgeSucceeded removes SUCCEEDED tasks older than olderThan (task_ttl
	// enforcement). Returns the count removed.
	PurgeSucceeded(ctx context.Context, queue string, olderThan time.Time) (int, error)

	// Capabilities reports the optional features this backend accelerates.
	Capabilities() Capabilities
}
