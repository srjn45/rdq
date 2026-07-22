// SPDX-License-Identifier: Apache-2.0

package postgres

// Capabilities (design 02 §2 "Optional capabilities", T2.5). With Enqueue
// (enqueue.go) completing the mandatory Storage floor, this file closes out the
// Postgres backend: it advertises the two optional accelerations the backend
// realizes and carries the compile-time proof that *Store satisfies spi.Storage.

import "github.com/srjn45/rdq/core/spi"

// Compile-time assertion that the Postgres backend implements the full Storage
// contract. Enqueue (enqueue.go, T2.5) supplies the last method; the claim,
// mutation, DLQ, and ops methods land in T2.3/T2.4. It lives here — the task that
// completes the interface — deliberately, as the prior branches each noted they
// could not assert it alone.
var _ spi.Storage = (*Store)(nil)

// Capabilities reports the optional features this backend accelerates (design 02
// §2). Postgres realizes both v1 accelerations: Notify via LISTEN/NOTIFY (a worker
// blocks in WaitDue instead of polling, notify.go) and FilterPushdown, where
// DLQFilter is evaluated natively against the denormalized, indexed rdq_dlq_task
// columns rather than paged and filtered in core (dlq.go, T2.4). BatchEnqueue is
// not implemented in v1.
func (s *Store) Capabilities() spi.Capabilities {
	return spi.Capabilities{Notify: true, FilterPushdown: true}
}
