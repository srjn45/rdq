// SPDX-License-Identifier: Apache-2.0

// Package submit builds task envelopes for enqueue — the submit half of the Go
// SDK, for a client that only produces work. It depends solely on the frozen
// core/envelope wire model and never on core/engine, any storage backend, or the
// worker runtime, so a service that merely submits tasks does not compile the
// whole engine into its binary (design 06 T4.1; the client/worker artifact split
// of design 05 §0.1). The worker-side registration API lives in a separate
// package (T4.2) that a submit-only client need not import.
//
// Submit assigns the task id itself rather than deferring it to storage, which is
// what makes a submit idempotent: two calls describing the same logical unit of
// work (same queue + idempotency key, via WithIdempotencyKey) produce the same
// id. Because storage enqueue is idempotent by id (design 02; core memstore
// T1.6), re-submitting after a crash, timeout, or ambiguous network error is
// safe and never duplicates the task.
package submit

import (
	"crypto/sha256"
	"fmt"
	"maps"
	"time"

	"github.com/srjn45/rdq/core/envelope"
)

// reservedHeaderPrefix is the header namespace reserved for system metadata;
// user-supplied headers must not use it (design 01 §2).
const reservedHeaderPrefix = "rdq."

// DefaultContentType tags a payload whose type the caller did not specify. The
// payload is opaque bytes, so the default is the generic octet-stream MIME type
// (design 01 §2); pass WithContentType to override.
const DefaultContentType = "application/octet-stream"

// options accumulates the optional inputs applied by the Option values passed to
// Submit. The zero value is the default submission (fresh id, no headers,
// DefaultContentType, wall-clock time).
type options struct {
	headers        map[string]string
	contentType    string
	handlerVersion string
	idempotencyKey string
	explicitID     string
	now            func() time.Time
}

// An Option customizes a Submit call.
type Option func(*options)

// WithHeaders adds the given headers to the task, merging with any set by earlier
// WithHeaders/WithHeader options (later keys win). Keys under the reserved "rdq."
// prefix are rejected by Submit.
func WithHeaders(h map[string]string) Option {
	return func(o *options) {
		if h == nil {
			return
		}
		if o.headers == nil {
			o.headers = make(map[string]string, len(h))
		}
		maps.Copy(o.headers, h)
	}
}

// WithHeader adds or overwrites a single header. Keys under the reserved "rdq."
// prefix are rejected by Submit.
func WithHeader(key, value string) Option {
	return func(o *options) {
		if o.headers == nil {
			o.headers = make(map[string]string, 1)
		}
		o.headers[key] = value
	}
}

// WithContentType sets payload_content_type. When unset (or empty) Submit uses
// DefaultContentType.
func WithContentType(ct string) Option {
	return func(o *options) { o.contentType = ct }
}

// WithHandlerVersion pins the handler_version the task expects; the engine's
// version-mismatch policy (T3.4) applies at claim time. Empty means unset.
func WithHandlerVersion(v string) Option {
	return func(o *options) { o.handlerVersion = v }
}

// WithIdempotencyKey derives the task id deterministically from the queue and
// key, so repeating a logical submit yields the same id and enqueue dedupes it.
// The queue is folded into the derivation, so the same key in two different
// queues yields two distinct ids — it never manufactures a cross-queue id
// collision (which storage would reject as ErrIDConflict, design 05 §0.1 G8).
// Mutually exclusive with WithID.
func WithIdempotencyKey(key string) Option {
	return func(o *options) { o.idempotencyKey = key }
}

// WithID sets the task id to an explicit, caller-chosen ULID — the other way to
// make a submit idempotent when the caller already has a stable id for the work.
// The value must be a valid ULID or Submit returns an error. Mutually exclusive
// with WithIdempotencyKey.
func WithID(id string) Option {
	return func(o *options) { o.explicitID = id }
}

// WithClock injects the time source Submit stamps into created_at,
// next_attempt_at, and a freshly generated id's timestamp. It exists for
// deterministic tests; production callers omit it and get time.Now.
func WithClock(now func() time.Time) Option {
	return func(o *options) { o.now = now }
}

// Submit builds a PENDING task envelope for queue, addressed to handlerRef, with
// the given opaque payload and optional headers/options. The returned envelope is
// due immediately (next_attempt_at == created_at) and carries no attempt history;
// the caller hands it to storage or an rdq-server. Submit does not enqueue —
// persistence is the storage layer's job — so this package stays free of any
// runtime dependency.
//
// The id is chosen as follows: an explicit WithID value (validated) if given,
// otherwise a deterministic id derived from WithIdempotencyKey, otherwise a fresh
// ULID stamped with the current time. The first two forms make the submit safely
// retryable; see the package doc.
func Submit(queue, handlerRef string, payload []byte, opts ...Option) (*envelope.Envelope, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	if err := envelope.ValidateQueue(queue); err != nil {
		return nil, err
	}
	if err := envelope.ValidateHandlerRef(handlerRef); err != nil {
		return nil, err
	}
	for k := range o.headers {
		if k == "" {
			return nil, fmt.Errorf("submit: invalid header: key must not be empty")
		}
		if len(k) >= len(reservedHeaderPrefix) && k[:len(reservedHeaderPrefix)] == reservedHeaderPrefix {
			return nil, fmt.Errorf("submit: invalid header %q: the %q prefix is reserved for system metadata", k, reservedHeaderPrefix)
		}
	}
	if o.explicitID != "" && o.idempotencyKey != "" {
		return nil, fmt.Errorf("submit: WithID and WithIdempotencyKey are mutually exclusive")
	}

	now := time.Now
	if o.now != nil {
		now = o.now
	}
	ts := now().UTC()

	id, err := resolveID(&o, queue, ts)
	if err != nil {
		return nil, err
	}

	contentType := o.contentType
	if contentType == "" {
		contentType = DefaultContentType
	}

	// Clone caller-owned inputs so later mutation of them cannot alter the built
	// envelope (and, for an idempotent retry, cannot desync the two copies).
	var headers map[string]string
	if len(o.headers) > 0 {
		headers = maps.Clone(o.headers)
	}
	var payloadCopy []byte
	if payload != nil {
		payloadCopy = make([]byte, len(payload))
		copy(payloadCopy, payload)
	}

	dueAt := ts
	e := &envelope.Envelope{
		EnvelopeVersion:    envelope.WriteVersion(),
		ID:                 id,
		Queue:              queue,
		HandlerRef:         handlerRef,
		HandlerVersion:     o.handlerVersion,
		Payload:            payloadCopy,
		PayloadContentType: contentType,
		Headers:            headers,
		Status:             envelope.StatusPending,
		AttemptCount:       0,
		RedriveCount:       0,
		NextAttemptAt:      &dueAt,
		LeaseExpiresAt:     nil,
		CreatedAt:          ts,
	}
	return e, nil
}

// resolveID picks the task id per the precedence documented on Submit: explicit
// id (validated) > idempotency-key derivation > fresh time-stamped ULID.
func resolveID(o *options, queue string, ts time.Time) (string, error) {
	switch {
	case o.explicitID != "":
		if _, err := envelope.ParseULID(o.explicitID); err != nil {
			return "", fmt.Errorf("submit: invalid id from WithID: %w", err)
		}
		return o.explicitID, nil
	case o.idempotencyKey != "":
		return deriveID(queue, o.idempotencyKey).String(), nil
	default:
		id, err := envelope.NewULIDAt(ts)
		if err != nil {
			return "", fmt.Errorf("submit: generating id: %w", err)
		}
		return id.String(), nil
	}
}

// deriveID maps a (queue, idempotencyKey) pair to a stable ULID. It hashes the
// pair with a domain separator and takes the leading 128 bits; every 16-byte
// value is a valid, round-trippable ULID (envelope.ULID.String always emits a
// timestamp high-nibble in range), so the result parses cleanly. The id is a
// pure function of its inputs — identical inputs always yield identical bytes —
// which is what makes a repeated submit idempotent.
func deriveID(queue, key string) envelope.ULID {
	sum := sha256.Sum256([]byte(queue + "\x00" + key))
	var id envelope.ULID
	copy(id[:], sum[:16])
	return id
}
