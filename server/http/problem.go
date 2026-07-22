// SPDX-License-Identifier: Apache-2.0

package http

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

// ProblemContentType is the media type for RFC 9457 error bodies (design 04
// conventions). Every error the API emits — routing, auth, or handler — uses
// it, so clients parse exactly one error shape.
const ProblemContentType = "application/problem+json"

// problemTypeBase is the stable prefix for a Problem's `type` URI. RFC 9457
// recommends a dereferenceable URI that documents the error class; the machine
// contract lives in `code`, so the human docs may lag without breaking clients.
const problemTypeBase = "https://rdq.dev/problems/"

// ProblemCode is a STABLE, machine-readable error identifier. These strings are
// part of the API contract (design 04 §conventions): clients switch on them, so
// their values MUST NOT change once shipped. Titles and HTTP statuses may be
// tuned; codes are frozen. Keep this set in sync with server/openapi.yaml — the
// spec-lint test asserts they match.
type ProblemCode string

const (
	// CodeNotFound — no route matches the request path (generic 404).
	CodeNotFound ProblemCode = "NOT_FOUND"
	// CodeMethodNotAllowed — the path exists but not for this method (405).
	CodeMethodNotAllowed ProblemCode = "METHOD_NOT_ALLOWED"
	// CodeUnauthenticated — missing/invalid credentials (401). Wired in T5.6.
	CodeUnauthenticated ProblemCode = "UNAUTHENTICATED"
	// CodeForbidden — authenticated but lacks the queue×role grant (403).
	CodeForbidden ProblemCode = "FORBIDDEN"
	// CodeQueueNotFound — the queue is not configured; never silently defaulted
	// (design 03 §3) (404).
	CodeQueueNotFound ProblemCode = "QUEUE_NOT_FOUND"
	// CodeIDConflict — task id reused across queues (design 04 §1, G8) (409).
	CodeIDConflict ProblemCode = "ID_CONFLICT"
	// CodePayloadTooLarge — payload exceeds the per-queue limit (413).
	CodePayloadTooLarge ProblemCode = "PAYLOAD_TOO_LARGE"
	// CodeInvalidTask — malformed submit body (422).
	CodeInvalidTask ProblemCode = "INVALID_TASK"
	// CodeStaleCursor — a pagination cursor can no longer be resolved; restart
	// paging (409).
	CodeStaleCursor ProblemCode = "STALE_CURSOR"
	// CodeConflict — generic state conflict, e.g. delete of a non-empty queue
	// (design 04 §3) (409).
	CodeConflict ProblemCode = "CONFLICT"
	// CodeRateLimited — request shed under load; retry after the hint (429,
	// carries Retry-After).
	CodeRateLimited ProblemCode = "RATE_LIMITED"
	// CodeStorageUnavailable — the storage backend is unreachable/degraded; the
	// operation is safe to retry (idempotent submit) (503, carries Retry-After).
	CodeStorageUnavailable ProblemCode = "STORAGE_UNAVAILABLE"
	// CodeInternal — an unexpected server fault (500).
	CodeInternal ProblemCode = "INTERNAL"
)

// problemDef fixes the HTTP status and default human title for a code. The code
// is the machine contract; status/title are presentation and may evolve.
type problemDef struct {
	status int
	title  string
}

// problemDefs is the single source of truth mapping every stable code to its
// status and title. The spec-lint test cross-checks the code set against
// server/openapi.yaml so the two never drift.
var problemDefs = map[ProblemCode]problemDef{
	CodeNotFound:           {http.StatusNotFound, "Not Found"},
	CodeMethodNotAllowed:   {http.StatusMethodNotAllowed, "Method Not Allowed"},
	CodeUnauthenticated:    {http.StatusUnauthorized, "Unauthenticated"},
	CodeForbidden:          {http.StatusForbidden, "Forbidden"},
	CodeQueueNotFound:      {http.StatusNotFound, "Queue Not Found"},
	CodeIDConflict:         {http.StatusConflict, "Task ID Conflict"},
	CodePayloadTooLarge:    {http.StatusRequestEntityTooLarge, "Payload Too Large"},
	CodeInvalidTask:        {http.StatusUnprocessableEntity, "Invalid Task"},
	CodeStaleCursor:        {http.StatusConflict, "Stale Cursor"},
	CodeConflict:           {http.StatusConflict, "Conflict"},
	CodeRateLimited:        {http.StatusTooManyRequests, "Rate Limited"},
	CodeStorageUnavailable: {http.StatusServiceUnavailable, "Storage Unavailable"},
	CodeInternal:           {http.StatusInternalServerError, "Internal Server Error"},
}

// Problem is an RFC 9457 problem+json body. `Code` is the stable machine
// contract; the remaining members are human-facing detail. Members serialize to
// the member names RFC 9457 defines, plus the `code` extension.
type Problem struct {
	// Type is a URI identifying the problem class (problemTypeBase + code).
	Type string `json:"type"`
	// Title is a short, human-readable summary of the problem class.
	Title string `json:"title"`
	// Status is the HTTP status code, duplicated in the body per RFC 9457 §3.1.
	Status int `json:"status"`
	// Detail is a human-readable explanation specific to this occurrence.
	Detail string `json:"detail,omitempty"`
	// Instance is a URI reference for this specific occurrence (the request path).
	Instance string `json:"instance,omitempty"`
	// Code is the STABLE machine-readable error code clients switch on.
	Code ProblemCode `json:"code"`

	// retryAfter, when > 0, is emitted as a Retry-After header (not in the body).
	// It is mandatory for 429 and 503 responses (see WriteProblem).
	retryAfter time.Duration
}

// ProblemOption customises a Problem before it is written.
type ProblemOption func(*Problem)

// WithDetail sets the occurrence-specific human explanation.
func WithDetail(detail string) ProblemOption {
	return func(p *Problem) { p.Detail = detail }
}

// WithRetryAfter sets the Retry-After hint. It is rounded up to whole seconds
// (the header is integer-seconds granularity) with a floor of one second for any
// positive duration, so a hint is never silently dropped to zero.
func WithRetryAfter(d time.Duration) ProblemOption {
	return func(p *Problem) { p.retryAfter = d }
}

// NewProblem builds a Problem for a stable code, applying options. Unknown codes
// fall back to CodeInternal so a miswired handler still emits a valid body.
func NewProblem(code ProblemCode, instance string, opts ...ProblemOption) *Problem {
	def, ok := problemDefs[code]
	if !ok {
		code, def = CodeInternal, problemDefs[CodeInternal]
	}
	p := &Problem{
		Type:     problemTypeBase + string(code),
		Title:    def.title,
		Status:   def.status,
		Instance: instance,
		Code:     code,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// WriteProblem serialises p as application/problem+json with its HTTP status.
//
// Retry-After invariant: any 429 or 503 response carries a Retry-After header
// (task T5.1 acceptance) — if the caller supplied no hint, a one-second default
// is used so the header is never absent on a retryable status.
func WriteProblem(w http.ResponseWriter, p *Problem) {
	retry := p.retryAfter
	if retry <= 0 && (p.Status == http.StatusTooManyRequests || p.Status == http.StatusServiceUnavailable) {
		retry = time.Second
	}
	if retry > 0 {
		secs := int(retry / time.Second)
		if retry%time.Second != 0 || secs == 0 {
			secs++ // round up; never advertise a zero-second wait
		}
		w.Header().Set("Retry-After", strconv.Itoa(secs))
	}
	w.Header().Set("Content-Type", ProblemContentType)
	w.WriteHeader(p.Status)
	// A trailing newline keeps curl/terminal output tidy; encoding cannot fail
	// for this closed struct.
	_ = json.NewEncoder(w).Encode(p)
}

// Error is the one-liner most handlers use: build a Problem for code against the
// request path and write it.
func Error(w http.ResponseWriter, r *http.Request, code ProblemCode, opts ...ProblemOption) {
	WriteProblem(w, NewProblem(code, r.URL.Path, opts...))
}
