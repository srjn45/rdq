// SPDX-License-Identifier: Apache-2.0

package spi

import (
	"time"

	"github.com/srjn45/rdq/core/envelope"
)

// TaskID identifies a task. It is the value of Envelope.ID; the alias lets an
// envelope's id flow through the Storage methods without conversion.
type TaskID = string

// Attempt is one execution record in a task's history. It is the wire type from
// package envelope, reused verbatim so outcome resolution appends the same
// records that travel in the envelope (design 01 §2).
type Attempt = envelope.Attempt

// ClaimToken is the fencing token minted by ClaimDue for a single claim. It
// authorizes exactly one live claim of a task: ExtendLease/Reschedule/Complete/
// DeadLetter reject any other token with ErrStaleClaim (design 02 §3). Opaque
// to the engine — its structure is the storage backend's concern.
type ClaimToken string

// Claimed is one task handed out by ClaimDue: the leased envelope (already
// IN_FLIGHT with LeaseExpiresAt set by the backend's clock, G9) paired with its
// fencing token.
type Claimed struct {
	Task  envelope.Envelope
	Token ClaimToken
}

// DLQFilter narrows a DLQList/Redrive/Purge selection over the dead-letter
// queue. Zero-valued fields are unconstrained. A backend advertising
// Capabilities.FilterPushdown evaluates it natively; otherwise core paginates
// and filters client-side.
type DLQFilter struct {
	// ErrorType matches the type string of the final (dead-lettering) attempt.
	ErrorType string `json:"error_type,omitempty"`
	// HandlerRef matches the task's handler_ref.
	HandlerRef string `json:"handler_ref,omitempty"`
	// DeadLetteredAfter/Before bound the dead-letter time range (inclusive
	// lower, exclusive upper); nil leaves that end open.
	DeadLetteredAfter  *time.Time `json:"dead_lettered_after,omitempty"`
	DeadLetteredBefore *time.Time `json:"dead_lettered_before,omitempty"`
	// IncludeAttempts requests full attempt histories in DLQList results.
	// Default false — histories make pages heavy (G13). Get always returns
	// full history regardless.
	IncludeAttempts bool `json:"include_attempts,omitempty"`
}

// Selector chooses tasks for Redrive/Purge: an explicit id set OR a DLQFilter,
// never both. An empty Selector selects nothing.
type Selector struct {
	IDs    []TaskID   `json:"ids,omitempty"`
	Filter *DLQFilter `json:"filter,omitempty"`
}

// Cursor is an opaque, backend-issued pagination token. The empty Cursor starts
// from the first page; DLQList returns the empty Cursor when a page is the last.
// A cursor that can no longer be resolved yields ErrStaleCursor.
type Cursor string

// Page is a pagination request: at most Limit entries starting at After. A zero
// or negative Limit lets the backend choose a default page size.
type Page struct {
	Limit int    `json:"limit,omitempty"`
	After Cursor `json:"after,omitempty"`
}

// Stats is a per-queue operational snapshot backing the Prometheus metrics
// (PRD FR-22).
type Stats struct {
	Pending          int64         `json:"pending"`
	InFlight         int64         `json:"in_flight"`
	DLQDepth         int64         `json:"dlq_depth"`
	OldestPendingAge time.Duration `json:"oldest_pending_age"`
}
