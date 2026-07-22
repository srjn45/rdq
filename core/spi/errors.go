// SPDX-License-Identifier: Apache-2.0

package spi

import "errors"

// Sentinel errors returned by Storage. Callers match with errors.Is.
var (
	// ErrStaleClaim is returned by ExtendLease/Reschedule/Complete/DeadLetter
	// when the supplied ClaimToken is no longer valid — the lease expired and
	// the task was reclaimed elsewhere. The operation changes nothing (design
	// 02 §3 fencing invariant); the handler must abandon its work.
	ErrStaleClaim = errors.New("spi: stale claim token")

	// ErrNotFound is returned by Get when no task with the given id exists in
	// any status.
	ErrNotFound = errors.New("spi: task not found")

	// ErrStaleCursor is returned by DLQList when a pagination Cursor can no
	// longer be resolved (e.g. it predates a purge). Callers restart paging.
	ErrStaleCursor = errors.New("spi: stale cursor")

	// ErrIDConflict is returned by Enqueue when the task id already exists in a
	// DIFFERENT queue. Re-enqueue within the SAME queue is an idempotent no-op;
	// a cross-queue collision is rejected rather than silently returning a
	// foreign envelope (G8). Maps to HTTP 409 at the API.
	ErrIDConflict = errors.New("spi: id exists in a different queue")
)
