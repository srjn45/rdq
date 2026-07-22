// SPDX-License-Identifier: Apache-2.0

// Package audit is the rdq audit-log seam (design 06, T6.3). It defines the
// AuditSink interface (G3) and the Record type that carries every DLQ mutation
// and API config change.
//
// The default sink (LogSink) writes one structured JSON line per record to any
// io.Writer, matching the core/log style. rdq-server additionally ships a
// Postgres sink in server/audit — both are wired through this interface so
// embedded adopters get the log sink for free and server deployments can opt
// into queryable audit history without changing the call sites.
//
// Audit stays entirely in the server/ops layer. The task-storage SPI
// (core/spi.Storage) is audit-free by design (T6.3 constraint).
package audit

import "time"

// Action names a DLQ or config-plane mutation. These are stable machine values
// (snake_case) suited for log-based alerting and audit queries (FR-18).
type Action string

const (
	ActionRedrive     Action = "redrive"
	ActionPurge       Action = "purge"
	ActionPause       Action = "pause"
	ActionResume      Action = "resume"
	ActionConfigWrite Action = "config_write"
)

// Outcome describes whether the audited operation completed successfully.
type Outcome string

const (
	OutcomeSuccess Outcome = "success"
	OutcomeFailure Outcome = "failure"
)

// Record is the atomic unit of audit history: one DLQ mutation or config
// change (FR-18, Config OI-2). All fields are populated by the call site; the
// Sink writes them durably.
type Record struct {
	// Timestamp is when the operation completed (UTC).
	Timestamp time.Time
	// Principal is the authenticated caller name ("anonymous" when auth is
	// disabled — embedded/dev mode).
	Principal string
	// Action is the kind of mutation.
	Action Action
	// Queue is the target queue. Empty for cross-queue operations.
	Queue string
	// Selector is a short human-readable description of what was selected
	// (e.g. "ids:[id1,id2]", "filter:{error_type:timeout}", "all").
	// Empty for operations that do not use a selector (pause/config_write).
	Selector string
	// Count is the number of tasks affected by a redrive or purge. -1 for
	// operations that do not produce a task count (pause/resume/config_write).
	Count int
	// Outcome reports whether the operation succeeded.
	Outcome Outcome
	// ErrorMessage holds the error string when Outcome is failure.
	ErrorMessage string
}

// Sink records audit events. Implementations must be safe for concurrent use.
// A nil Sink is treated as a no-op so call sites may hold an optional Sink
// and emit unconditionally without nil checks.
type Sink interface {
	// Emit records one audit event. Errors are implementation-defined;
	// callers log them but do not propagate them to the API response so a
	// sink failure never blocks an otherwise-successful operation.
	Emit(r Record) error
}

// Discard returns a Sink that silently drops every record — for tests and
// embedded mode when auditing is intentionally disabled.
func Discard() Sink { return discardSink{} }

type discardSink struct{}

func (discardSink) Emit(Record) error { return nil }
