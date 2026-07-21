// SPDX-License-Identifier: Apache-2.0

package callback

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/srjn45/rdq/core/config"
	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/policy"
)

// Outbound header names on a callback request (design 04 §4). The receiver sees
// its own payload natively as the body; task metadata rides in these headers so
// there is nothing to unwrap.
const (
	HeaderTaskID     = "X-RDQ-Task-Id"
	HeaderQueue      = "X-RDQ-Queue"
	HeaderHandlerRef = "X-RDQ-Handler-Ref"
	HeaderAttempt    = "X-RDQ-Attempt"
)

// maxDetailBytes caps how much of a failed callback's response body is captured
// into the attempt's error.detail (design 04 §4: first 4 KiB) — enough for a
// callback returning `500 {"reason": "..."}` to produce a self-explanatory DLQ
// entry, without letting a chatty receiver bloat the stored envelope.
const maxDetailBytes = 4 << 10

// truncationMarker is appended to a captured body that exceeded maxDetailBytes,
// matching the field-length truncation sentinel of design 05.
const truncationMarker = "…[truncated]"

// defaultContentType is used when a task carries no payload_content_type.
const defaultContentType = "application/octet-stream"

// AuthMode selects how a callback request authenticates itself to the receiver
// (design 04 §4). It is this package's own enum: the server wiring maps a
// queue's callback.auth block onto it, dereferencing secret_ref to Secret.
type AuthMode string

const (
	// AuthNone sends no credential.
	AuthNone AuthMode = "none"
	// AuthBearer sends `Authorization: Bearer <secret>`.
	AuthBearer AuthMode = "bearer"
	// AuthHeader sends the raw secret in a caller-named header.
	AuthHeader AuthMode = "header"
	// AuthHMAC signs the request body under secret and sends the signature in
	// SignatureHeader; it adds no Authorization header.
	AuthHMAC AuthMode = "hmac"
)

// Target is a resolved callback endpoint: a queue's validated callback block
// with its secret_ref already dereferenced to bytes. secret_ref resolution and
// the SSRF allowlist check live in the server wiring (design 03 §5, T5.6); the
// dispatcher operates on concrete values.
type Target struct {
	// URL is the absolute callback endpoint; the request is POSTed here.
	URL string
	// ContentType becomes the request's Content-Type (the task's
	// payload_content_type); empty defaults to application/octet-stream.
	ContentType string
	// Timeout bounds a single dispatch. A timeout is a retryable TIMEOUT
	// outcome (design 04 §4). Zero means no dispatch-level deadline.
	Timeout time.Duration

	// Auth selects the credential scheme; the empty value is treated as AuthNone.
	Auth AuthMode
	// HeaderName is the header carrying Secret when Auth is AuthHeader.
	HeaderName string
	// Secret is the bearer token, header value, or HMAC key per Auth.
	Secret []byte

	// ResponseMapping overrides the FR-29 default status classification per
	// queue (design 03 §2); nil applies the defaults alone.
	ResponseMapping *config.ResponseMapping
}

// Task is the unit delivered to a callback: the raw payload plus the metadata
// that rides in X-RDQ-* headers.
type Task struct {
	ID         string
	Queue      string
	HandlerRef string
	// Attempt is the 1-based attempt number for this delivery.
	Attempt int
	// Payload is delivered as the request body byte-for-byte — no re-encoding.
	Payload []byte
	// Headers are propagated onto the request verbatim (e.g. traceparent). They
	// are set before the RDQ-controlled headers, so they can never shadow an
	// X-RDQ-* header, the signature, or the auth credential.
	Headers map[string]string
}

// Result is the classified outcome of one callback delivery. Outcome is what the
// worker records on the attempt; it is projected from a policy.Decision via
// Decision.Outcome() for failures (design 01 §2) and is OutcomeSuccess for a 2xx
// ack.
type Result struct {
	// Outcome is SUCCESS, RETRYABLE_FAILURE, or PERMANENT_FAILURE.
	Outcome envelope.Outcome
	// Status is the HTTP response status, or 0 when no response was received
	// (timeout or transport error).
	Status int
	// Error carries the failure detail to attach to the attempt; nil on success.
	Error *envelope.Error
}

// Success reports whether the callback acked (a 2xx response).
func (r Result) Success() bool { return r.Outcome == envelope.OutcomeSuccess }

// Dispatcher delivers callbacks over a shared http.Client. It is safe for
// concurrent use.
type Dispatcher struct {
	client *http.Client
	now    func() time.Time
}

// Option configures a Dispatcher.
type Option func(*Dispatcher)

// WithClient overrides the HTTP client (tests inject a stub-backed transport).
// The client's redirect policy is left as given; New's default declines to
// follow redirects so a 3xx is classified rather than chased.
func WithClient(c *http.Client) Option { return func(d *Dispatcher) { d.client = c } }

// WithClock overrides the time source used for signature timestamps (tests pin
// it for reproducible signatures).
func WithClock(now func() time.Time) Option { return func(d *Dispatcher) { d.now = now } }

// New builds a Dispatcher. Per-target timeouts are applied through the request
// context rather than http.Client.Timeout, so one client serves targets with
// differing deadlines. Redirects are not followed: a callback that answers 3xx
// is a misbehaving receiver, and the response should be classified, not chased
// to an off-allowlist host (SSRF hygiene).
func New(opts ...Option) *Dispatcher {
	d := &Dispatcher{
		client: &http.Client{
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		now: time.Now,
	}
	for _, o := range opts {
		o(d)
	}
	return d
}

// Dispatch delivers task to target and classifies the response. A non-nil error
// means the request could not be built (a misconfiguration) and no delivery was
// attempted; every actual delivery outcome — 2xx/4xx/5xx, timeout, transport
// failure — is reported in Result with a nil error, because a failed callback is
// a task outcome, not a program fault.
func (d *Dispatcher) Dispatch(ctx context.Context, target Target, task Task) (Result, error) {
	if target.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, target.Timeout)
		defer cancel()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.URL, bytes.NewReader(task.Payload))
	if err != nil {
		return Result{}, fmt.Errorf("callback: build request: %w", err)
	}
	d.setHeaders(req, target, task)

	resp, err := d.client.Do(req)
	if err != nil {
		return d.transportFailure(err), nil
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		// v1 treats any 2xx as an ack; body_mapper (200-with-error APIs) is
		// post-v1 (design 03 §2).
		return Result{Outcome: envelope.OutcomeSuccess, Status: resp.StatusCode}, nil
	}

	decision := classifyStatus(resp.StatusCode, target.ResponseMapping)
	return Result{
		Outcome: decision.Outcome(),
		Status:  resp.StatusCode,
		Error: &envelope.Error{
			Type:    fmt.Sprintf("HTTP_%d", resp.StatusCode),
			Message: fmt.Sprintf("callback returned %d %s", resp.StatusCode, http.StatusText(resp.StatusCode)),
			Detail:  captureDetail(resp.Body),
		},
	}, nil
}

// setHeaders writes propagated task headers first, then the RDQ-controlled
// headers, so ours always win over anything a task tried to smuggle in.
func (d *Dispatcher) setHeaders(req *http.Request, target Target, task Task) {
	for k, v := range task.Headers {
		req.Header.Set(k, v)
	}

	ct := target.ContentType
	if ct == "" {
		ct = defaultContentType
	}
	req.Header.Set("Content-Type", ct)
	req.Header.Set(HeaderTaskID, task.ID)
	req.Header.Set(HeaderQueue, task.Queue)
	req.Header.Set(HeaderHandlerRef, task.HandlerRef)
	req.Header.Set(HeaderAttempt, strconv.Itoa(task.Attempt))

	switch target.Auth {
	case AuthBearer:
		req.Header.Set("Authorization", "Bearer "+string(target.Secret))
	case AuthHeader:
		if target.HeaderName != "" {
			req.Header.Set(target.HeaderName, string(target.Secret))
		}
	case AuthHMAC:
		req.Header.Set(SignatureHeader, Sign(target.Secret, task.Payload, d.now().Unix()))
	}
}

// transportFailure maps a client.Do error onto a retryable outcome. A timeout is
// the named TIMEOUT type (design 04 §4); any other transport error (dial refused,
// reset, TLS) is likewise transient and retryable, under a stable TRANSPORT_ERROR
// type. No response arrived, so Status is 0.
func (d *Dispatcher) transportFailure(err error) Result {
	errType := "TRANSPORT_ERROR"
	if isTimeout(err) {
		errType = "TIMEOUT"
	}
	return Result{
		Outcome: policy.DecisionRetryable.Outcome(),
		Status:  0,
		Error: &envelope.Error{
			Type:    errType,
			Message: err.Error(),
		},
	}
}

// isTimeout reports whether err is a request deadline being exceeded — either the
// context deadline (our per-target Timeout) or a lower-level net timeout.
func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// classifyStatus maps a non-2xx HTTP status onto a retry/terminal Decision,
// reusing T3.3's policy.Decision rather than a parallel enum. FR-29 defaults —
// 4xx terminal, 5xx retryable, anything else retryable — apply unless the queue's
// response_mapping overrides them.
//
// Override precedence is by specificity, so the canonical mapping
// (retryable_status: [408, 429, "5xx"], permanent_status: ["4xx"]) does the
// intuitive thing: an exact code (408 retryable) beats a class glob (4xx
// permanent). Within a specificity tier the permanent list is consulted first,
// so an explicit "never retry this" — a poison-pill bound — is never silently
// undone by an also-matching retryable rule (mirrors policy.matchGlobs).
func classifyStatus(status int, rm *config.ResponseMapping) policy.Decision {
	if rm != nil {
		// Tier 1: exact-code matchers, most specific.
		if matchExact(rm.PermanentStatus, status) {
			return policy.DecisionPermanent
		}
		if matchExact(rm.RetryableStatus, status) {
			return policy.DecisionRetryable
		}
		// Tier 2: class matchers (4xx, 5xx).
		if matchClass(rm.PermanentStatus, status) {
			return policy.DecisionPermanent
		}
		if matchClass(rm.RetryableStatus, status) {
			return policy.DecisionRetryable
		}
	}
	// FR-29 defaults.
	if status >= 400 && status < 500 {
		return policy.DecisionPermanent
	}
	// 5xx and any other non-2xx (1xx/3xx): retryable, the ladder default.
	return policy.DecisionRetryable
}

// matchExact reports whether an exact-code matcher in list equals status.
func matchExact(list []config.StatusMatcher, status int) bool {
	for _, m := range list {
		if m.Code > 0 && m.Matches(status) {
			return true
		}
	}
	return false
}

// matchClass reports whether a class matcher (e.g. "5xx") in list covers status.
func matchClass(list []config.StatusMatcher, status int) bool {
	for _, m := range list {
		if m.Code == 0 && m.Matches(status) {
			return true
		}
	}
	return false
}

// captureDetail reads up to maxDetailBytes of a failed callback's response body
// into the attempt's error.detail. envelope.Error.Detail is JSON, so a body that
// is already valid JSON is stored verbatim (the common `{"reason": "..."}`
// case); any other body is wrapped as a JSON string so the envelope still
// round-trips. An over-length body is truncated with the design-05 sentinel.
func captureDetail(body io.Reader) json.RawMessage {
	// Read one extra byte to detect overflow past the cap.
	raw, _ := io.ReadAll(io.LimitReader(body, maxDetailBytes+1))
	if len(raw) == 0 {
		return nil
	}
	truncated := len(raw) > maxDetailBytes
	if truncated {
		raw = raw[:maxDetailBytes]
	}
	if !truncated && json.Valid(raw) {
		return json.RawMessage(raw)
	}
	s := string(raw)
	if truncated {
		s += truncationMarker
	}
	quoted, _ := json.Marshal(s)
	return quoted
}
