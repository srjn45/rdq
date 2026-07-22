// SPDX-License-Identifier: Apache-2.0

package log

import (
	"context"
	"strings"
)

// HeaderTraceparent is the header key the W3C trace context rides under, both in
// an Envelope's headers map (design 01 §2 — "traceparent rides here") and as the
// HTTP request/callback header of the same name. Lower-case per the W3C spec.
const HeaderTraceparent = "traceparent"

// traceCtxKey is the private context key carrying the active traceparent through
// a handler invocation so downstream calls (and transition logs) can read it.
type traceCtxKey struct{}

// ContextWithTraceparent returns ctx carrying tp as the active W3C trace context.
// An empty tp is a no-op so callers need not special-case the untraced path. The
// value is stored verbatim (not validated) — validation happens at parse time so
// a malformed inbound traceparent is still propagated for debugging but never
// yields a bogus trace_id in logs.
func ContextWithTraceparent(ctx context.Context, tp string) context.Context {
	if tp == "" {
		return ctx
	}
	return context.WithValue(ctx, traceCtxKey{}, tp)
}

// TraceparentFromContext returns the traceparent carried by ctx, or "" if none.
func TraceparentFromContext(ctx context.Context) string {
	tp, _ := ctx.Value(traceCtxKey{}).(string)
	return tp
}

// TraceparentFromHeaders returns the traceparent stored in a task's headers map
// (where trace context is persisted across retries and redrive), or "" if absent.
func TraceparentFromHeaders(headers map[string]string) string {
	if headers == nil {
		return ""
	}
	return headers[HeaderTraceparent]
}

// effectiveTraceparent resolves the traceparent to log for a transition: an
// active span in ctx takes precedence over the persisted task headers, so a
// handler that started a child span still logs against the live context.
func effectiveTraceparent(ctx context.Context, headers map[string]string) string {
	if tp := TraceparentFromContext(ctx); tp != "" {
		return tp
	}
	return TraceparentFromHeaders(headers)
}

// ParseTraceparent parses a W3C traceparent value of the form
// "version-trace_id-parent_id-trace_flags" (RFC: 2-32-16-2 hex chars) and returns
// the trace_id and parent_id (span_id). ok is false for any malformed value or an
// all-zero trace_id/parent_id (the spec's "invalid" sentinels), so callers never
// emit a meaningless trace_id. Only the "00" version is recognised; a future
// version with the same field prefix still parses (forward-compatible), an
// unknown SHAPE does not.
func ParseTraceparent(tp string) (traceID, spanID string, ok bool) {
	parts := strings.Split(tp, "-")
	if len(parts) != 4 {
		return "", "", false
	}
	version, tid, sid, flags := parts[0], parts[1], parts[2], parts[3]
	if !isHex(version, 2) || !isHex(tid, 32) || !isHex(sid, 16) || !isHex(flags, 2) {
		return "", "", false
	}
	if isAllZero(tid) || isAllZero(sid) {
		return "", "", false
	}
	return tid, sid, true
}

// isHex reports whether s is exactly n lowercase-hex characters.
func isHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// isAllZero reports whether s is entirely '0' characters.
func isAllZero(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] != '0' {
			return false
		}
	}
	return true
}
