// SPDX-License-Identifier: Apache-2.0

// Package envelope is the language-neutral wire model of a task: the single
// contract shared by the SDKs, rdq-server, the CLI, and every storage plugin
// (design 01). This file defines the struct shape and snake_case JSON tags; the
// canonical RFC-3339-millis / base64 codec is layered on in a later task.
package envelope

import (
	"encoding/json"
	"time"
)

// Envelope is the full task representation (design 01 §2). Timestamps are plain
// time.Time here; the canonical millisecond-precision codec arrives in T1.2.
type Envelope struct {
	EnvelopeVersion int    `json:"envelope_version"`
	ID              string `json:"id"`
	Queue           string `json:"queue"`
	HandlerRef      string `json:"handler_ref"`
	HandlerVersion  string `json:"handler_version,omitempty"`

	// Payload is opaque bytes (base64 in JSON), tagged by PayloadContentType.
	Payload            []byte `json:"payload"`
	PayloadContentType string `json:"payload_content_type"`

	// PayloadRef is reserved for the post-v1 claim-check seam (design 05,
	// OQ-2): an object-storage pointer for oversized payloads. Documented but
	// unused in v1 — do not populate it.
	PayloadRef *string `json:"payload_ref,omitempty"`

	// Headers is a flat string→string map. The "rdq." prefix is reserved for
	// system metadata (design 01 §2).
	Headers map[string]string `json:"headers,omitempty"`

	Status       Status `json:"status"`
	AttemptCount int    `json:"attempt_count"`
	RedriveCount int    `json:"redrive_count"`

	// NextAttemptAt is when the task becomes due; null once terminal.
	NextAttemptAt *time.Time `json:"next_attempt_at"`
	// LeaseExpiresAt is set while IN_FLIGHT; null otherwise.
	LeaseExpiresAt *time.Time `json:"lease_expires_at"`
	CreatedAt      time.Time  `json:"created_at"`

	// Attempts is the ordered failure history; it travels into the DLQ.
	Attempts []Attempt `json:"attempts,omitempty"`
}

// Attempt is one execution record in an Envelope's history (design 01 §2).
type Attempt struct {
	AttemptNo int       `json:"attempt_no"`
	StartedAt time.Time `json:"started_at"`
	// FinishedAt is null while the attempt is still in flight.
	FinishedAt *time.Time `json:"finished_at"`
	Outcome    Outcome    `json:"outcome"`
	Error      *Error     `json:"error,omitempty"`
}

// Error is the failure detail attached to a non-successful attempt (design 01
// §2). Type is the language-native class/type string; for a LEASE_EXPIRED
// attempt it is "rdq.LeaseExpired" (design 05, G6/G7).
type Error struct {
	Type    string `json:"type"`
	Message string `json:"message"`
	// Detail is optional structured JSON supplied by an OutcomeMapper.
	Detail json.RawMessage `json:"detail,omitempty"`
	// Stack is optional and truncated at 64 KiB by the writer.
	Stack string `json:"stack,omitempty"`
}
