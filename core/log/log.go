// SPDX-License-Identifier: Apache-2.0

// Package log is rdq's structured-logging seam (design 06, T6.2). It is a thin
// wrapper over the standard library log/slog that gives the engine and server a
// single, consistent way to emit one structured record on every task state
// transition — always carrying the task id and queue, the attempt count, and the
// W3C trace_id when a traceparent is in flight (submit → retry → handler).
//
// FR-25 is enforced here structurally: payloads are treated as sensitive and are
// NEVER logged in full. The only payload facts that reach a log are its byte
// length and a SHA-256 hash (PayloadAttrs) — enough to correlate identical
// payloads and reason about size, never enough to reconstruct the bytes.
package log

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"

	"github.com/srjn45/rdq/core/envelope"
)

// Transition names the state change a log record describes. These are stable
// machine values (snake_case) suitable for log-based alerting and dashboards.
type Transition string

const (
	// TransitionClaimed: a due task was leased and is about to run.
	TransitionClaimed Transition = "claimed"
	// TransitionSucceeded: a handler completed and the task is SUCCEEDED.
	TransitionSucceeded Transition = "succeeded"
	// TransitionRetried: a retryable failure scheduled the next attempt.
	TransitionRetried Transition = "retry_scheduled"
	// TransitionDeadLettered: a permanent/exhausted failure moved the task to DLQ.
	TransitionDeadLettered Transition = "dead_lettered"
	// TransitionAbandoned: an outcome write was rejected (lost lease / stale
	// claim); the task will be reclaimed elsewhere (at-least-once).
	TransitionAbandoned Transition = "abandoned"
)

// Stable structured keys. Kept as constants so producers (engine/server) and any
// consumers (tests, log pipelines) agree on the field names.
const (
	KeyTransition    = "transition"
	KeyTaskID        = "task_id"
	KeyQueue         = "queue"
	KeyAttemptCount  = "attempt_count"
	KeyStatus        = "status"
	KeyTraceID       = "trace_id"
	KeySpanID        = "span_id"
	KeyErrorType     = "error_type"
	KeyNextAttemptAt = "next_attempt_at"
	KeyPayloadBytes  = "payload_bytes"
	KeyPayloadSHA256 = "payload_sha256"
)

// transitionMsg is the slog message shared by every transition record; the
// machine-meaningful discriminator is the KeyTransition attribute.
const transitionMsg = "task.transition"

// Logger is rdq's structured logger. The zero value and a nil *Logger are both
// safe no-ops, so callers may hold an optional *Logger and emit unconditionally
// without nil checks. Construct one with New, NewFromSlog, or Discard.
type Logger struct {
	inner *slog.Logger
}

// New returns a Logger writing JSON records to w at Info level — the default
// production sink (e.g. os.Stderr).
func New(w io.Writer) *Logger {
	return &Logger{inner: slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo}))}
}

// NewFromSlog adapts an existing *slog.Logger (letting an embedder supply its own
// handler, level, and base attributes). A nil argument yields a no-op Logger.
func NewFromSlog(l *slog.Logger) *Logger {
	if l == nil {
		return nil
	}
	return &Logger{inner: l}
}

// Discard returns a Logger that drops every record — handy in tests and when
// logging is intentionally disabled.
func Discard() *Logger { return &Logger{inner: slog.New(slog.NewJSONHandler(io.Discard, nil))} }

// Slog exposes the underlying *slog.Logger for callers that want to log
// non-transition events (e.g. request logs) with the same handler. Returns nil
// for a no-op Logger.
func (l *Logger) Slog() *slog.Logger {
	if l == nil {
		return nil
	}
	return l.inner
}

// Transition emits exactly one structured record for a task state change. It
// always includes the task id and queue (the required identity, T6.2), the
// attempt count and status, and — when a traceparent is present in ctx or the
// task headers — the W3C trace_id and span_id (parent_id). Payload facts are
// added via PayloadAttrs: size and hash only, never the bytes (FR-25). Extra
// attrs (e.g. error_type, next_attempt_at) are appended by the caller.
//
// A nil Logger is a no-op, so the engine can call this on every transition
// whether or not logging is configured.
func (l *Logger) Transition(ctx context.Context, t Transition, env envelope.Envelope, attrs ...slog.Attr) {
	if l == nil || l.inner == nil {
		return
	}
	rec := make([]slog.Attr, 0, 8+len(attrs))
	rec = append(rec,
		slog.String(KeyTransition, string(t)),
		slog.String(KeyTaskID, env.ID),
		slog.String(KeyQueue, env.Queue),
		slog.Int(KeyAttemptCount, env.AttemptCount),
		slog.String(KeyStatus, string(env.Status)),
	)
	if tp := effectiveTraceparent(ctx, env.Headers); tp != "" {
		if tid, sid, ok := ParseTraceparent(tp); ok {
			rec = append(rec, slog.String(KeyTraceID, tid), slog.String(KeySpanID, sid))
		}
	}
	rec = append(rec, PayloadAttrs(env.Payload)...)
	rec = append(rec, attrs...)
	l.inner.LogAttrs(ctx, slog.LevelInfo, transitionMsg, rec...)
}

// PayloadAttrs returns the ONLY log-safe facts about a payload (FR-25): its byte
// length, and — for a non-empty payload — a SHA-256 hex digest so identical
// payloads correlate across records. The raw bytes never appear in the result,
// so a caller cannot accidentally log them by splatting these attrs.
func PayloadAttrs(payload []byte) []slog.Attr {
	attrs := []slog.Attr{slog.Int(KeyPayloadBytes, len(payload))}
	if len(payload) > 0 {
		sum := sha256.Sum256(payload)
		attrs = append(attrs, slog.String(KeyPayloadSHA256, hex.EncodeToString(sum[:])))
	}
	return attrs
}
