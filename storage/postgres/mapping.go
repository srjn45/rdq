// SPDX-License-Identifier: Apache-2.0

package postgres

// Envelope ↔ rows mapping (design 02 §4, T2.2). The wire envelope (design 01 §2)
// decomposes losslessly into the columns of rdq_task / rdq_dlq_task plus a row
// per attempt in rdq_attempt; unknown top-level and per-attempt JSON fields ride
// along in the `residual` JSONB columns so a task written by a newer
// envelope_version round-trips through an older engine (design 01 §5, rule 1).
//
// This file is pure column projection — no SQL, no clock, no fencing. The claim
// and mutation layers (T2.3+) own the storage-managed columns that are NOT part
// of the envelope: `claim_token` (fencing), and the DLQ-only denormalized
// `dead_lettered_at` / `error_type` (filter pushdown). terminalErrorType derives
// the last of those from the attempt history for the DeadLetter path.

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/srjn45/rdq/core/envelope"
)

// taskRow is the envelope-derived column projection shared by rdq_task and
// rdq_dlq_task (the two tables carry the same envelope columns; only the
// denormalized DLQ columns and the CHECK on `status` differ). Nullable columns
// use pointers; `headers` and `residual` are JSONB byte payloads.
type taskRow struct {
	ID                 string
	Queue              string
	EnvelopeVersion    int
	HandlerRef         string
	HandlerVersion     *string
	Payload            []byte
	PayloadContentType string
	PayloadRef         *string
	Headers            []byte // jsonb; always a valid object, "{}" when empty
	Status             string
	AttemptCount       int
	RedriveCount       int
	NextAttemptAt      *time.Time
	LeaseExpiresAt     *time.Time
	CreatedAt          time.Time
	Residual           []byte // jsonb; always a valid object, "{}" when empty
}

// attemptRow is one rdq_attempt row: the flattened columns of one Attempt
// (design 01 §2). The Error sub-object is decomposed into error_* columns so the
// terminal error type is indexable for DLQFilter pushdown; error_detail is JSONB.
type attemptRow struct {
	TaskID       string
	AttemptNo    int
	StartedAt    time.Time
	FinishedAt   *time.Time
	Outcome      string
	ErrorType    *string
	ErrorMessage *string
	ErrorDetail  []byte // jsonb; null when the attempt carries no error detail
	ErrorStack   *string
	Residual     []byte // jsonb; always a valid object, "{}" when empty
}

// taskRowFromEnvelope decomposes e into the shared envelope columns. It leaves
// the storage-managed columns (claim_token, and the DLQ denormalizations) to the
// mutation layer; the returned row is what INSERT/UPDATE binds for the envelope
// portion of either task table.
func taskRowFromEnvelope(e *envelope.Envelope) (taskRow, error) {
	headers, err := encodeStringMap(e.Headers)
	if err != nil {
		return taskRow{}, fmt.Errorf("rdq/postgres: encoding headers: %w", err)
	}
	residual, err := encodeResidual(e.Residual)
	if err != nil {
		return taskRow{}, fmt.Errorf("rdq/postgres: encoding residual: %w", err)
	}
	return taskRow{
		ID:                 e.ID,
		Queue:              e.Queue,
		EnvelopeVersion:    e.EnvelopeVersion,
		HandlerRef:         e.HandlerRef,
		HandlerVersion:     emptyToNil(e.HandlerVersion),
		Payload:            e.Payload,
		PayloadContentType: e.PayloadContentType,
		PayloadRef:         e.PayloadRef,
		Headers:            headers,
		Status:             string(e.Status),
		AttemptCount:       e.AttemptCount,
		RedriveCount:       e.RedriveCount,
		NextAttemptAt:      e.NextAttemptAt,
		LeaseExpiresAt:     e.LeaseExpiresAt,
		CreatedAt:          e.CreatedAt,
		Residual:           residual,
	}, nil
}

// attemptRowsFromEnvelope decomposes e's attempt history into rdq_attempt rows,
// each stamped with the task id. The order of e.Attempts is preserved; attempt_no
// carries the true sequence for stable re-assembly.
func attemptRowsFromEnvelope(e *envelope.Envelope) ([]attemptRow, error) {
	if len(e.Attempts) == 0 {
		return nil, nil
	}
	rows := make([]attemptRow, len(e.Attempts))
	for i, a := range e.Attempts {
		residual, err := encodeResidual(a.Residual)
		if err != nil {
			return nil, fmt.Errorf("rdq/postgres: encoding attempt %d residual: %w", a.AttemptNo, err)
		}
		row := attemptRow{
			TaskID:     e.ID,
			AttemptNo:  a.AttemptNo,
			StartedAt:  a.StartedAt,
			FinishedAt: a.FinishedAt,
			Outcome:    string(a.Outcome),
			Residual:   residual,
		}
		if a.Error != nil {
			t := a.Error.Type
			m := a.Error.Message
			row.ErrorType = &t
			row.ErrorMessage = &m
			row.ErrorStack = emptyToNil(a.Error.Stack)
			if len(a.Error.Detail) > 0 {
				row.ErrorDetail = a.Error.Detail
			}
		}
		rows[i] = row
	}
	return rows, nil
}

// envelopeFromRows reassembles an Envelope from a task row and its attempt rows.
// It is the inverse of taskRowFromEnvelope + attemptRowsFromEnvelope: for any
// valid envelope, envelopeFromRows(taskRowFromEnvelope(e), attemptRowsFromEnvelope(e))
// re-encodes to the same canonical bytes as e (design 01 §1). Attempt rows are
// assembled in the given order; callers query them ORDER BY attempt_no.
func envelopeFromRows(t taskRow, attempts []attemptRow) (*envelope.Envelope, error) {
	headers, err := decodeStringMap(t.Headers)
	if err != nil {
		return nil, fmt.Errorf("rdq/postgres: decoding headers: %w", err)
	}
	residual, err := decodeResidual(t.Residual)
	if err != nil {
		return nil, fmt.Errorf("rdq/postgres: decoding residual: %w", err)
	}
	e := &envelope.Envelope{
		EnvelopeVersion:    t.EnvelopeVersion,
		ID:                 t.ID,
		Queue:              t.Queue,
		HandlerRef:         t.HandlerRef,
		HandlerVersion:     nilToEmpty(t.HandlerVersion),
		Payload:            t.Payload,
		PayloadContentType: t.PayloadContentType,
		PayloadRef:         t.PayloadRef,
		Headers:            headers,
		Status:             envelope.Status(t.Status),
		AttemptCount:       t.AttemptCount,
		RedriveCount:       t.RedriveCount,
		NextAttemptAt:      t.NextAttemptAt,
		LeaseExpiresAt:     t.LeaseExpiresAt,
		CreatedAt:          t.CreatedAt,
		Residual:           residual,
	}
	if len(attempts) > 0 {
		e.Attempts = make([]envelope.Attempt, len(attempts))
		for i, r := range attempts {
			ar, err := attemptFromRow(r)
			if err != nil {
				return nil, err
			}
			e.Attempts[i] = ar
		}
	}
	return e, nil
}

// attemptFromRow reassembles one Attempt from its row, rebuilding the Error
// sub-object only when the row carries error columns. A NULL error_type marks an
// attempt with no error (e.g. a SUCCESS outcome); every rdq Error carries a
// non-empty type (design 05 G6), so error_type presence is the reliable signal.
func attemptFromRow(r attemptRow) (envelope.Attempt, error) {
	residual, err := decodeResidual(r.Residual)
	if err != nil {
		return envelope.Attempt{}, fmt.Errorf("rdq/postgres: decoding attempt %d residual: %w", r.AttemptNo, err)
	}
	a := envelope.Attempt{
		AttemptNo:  r.AttemptNo,
		StartedAt:  r.StartedAt,
		FinishedAt: r.FinishedAt,
		Outcome:    envelope.Outcome(r.Outcome),
		Residual:   residual,
	}
	if r.ErrorType != nil {
		a.Error = &envelope.Error{
			Type:    *r.ErrorType,
			Message: nilToEmpty(r.ErrorMessage),
			Stack:   nilToEmpty(r.ErrorStack),
		}
		if len(r.ErrorDetail) > 0 {
			a.Error.Detail = json.RawMessage(r.ErrorDetail)
		}
	}
	return a, nil
}

// terminalErrorType returns the type of the last attempt's error, the value the
// DeadLetter mutation denormalizes into rdq_dlq_task.error_type for DLQFilter
// pushdown. It is nil when there is no attempt history or the final attempt
// carried no error.
func terminalErrorType(e *envelope.Envelope) *string {
	if len(e.Attempts) == 0 {
		return nil
	}
	last := e.Attempts[len(e.Attempts)-1]
	if last.Error == nil {
		return nil
	}
	t := last.Error.Type
	return &t
}

// encodeStringMap marshals a string→string map to a JSONB object payload,
// returning "{}" (never nil) for an empty map so the NOT NULL headers column is
// always satisfiable.
func encodeStringMap(m map[string]string) ([]byte, error) {
	if len(m) == 0 {
		return []byte("{}"), nil
	}
	return json.Marshal(m)
}

// decodeStringMap parses a JSONB object payload back into a string→string map,
// collapsing an empty object (or empty input) to a nil map so it re-marshals with
// omitempty exactly as it was written.
func decodeStringMap(b []byte) (map[string]string, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var m map[string]string
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	if len(m) == 0 {
		return nil, nil
	}
	return m, nil
}

// encodeResidual marshals a residual map to a JSONB object payload. Keys are
// emitted sorted (encoding/json orders map keys), so the stored bytes are
// deterministic; values are re-emitted verbatim (json.RawMessage). Empty →
// "{}" so the NOT NULL residual column is always satisfiable.
func encodeResidual(r map[string]json.RawMessage) ([]byte, error) {
	if len(r) == 0 {
		return []byte("{}"), nil
	}
	return json.Marshal(r)
}

// decodeResidual parses a JSONB residual payload back into a residual map,
// collapsing an empty object to nil so an envelope with no unknown fields keeps a
// nil Residual (matching the codec) and re-encodes byte-for-byte.
func decodeResidual(b []byte) (map[string]json.RawMessage, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var r map[string]json.RawMessage
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, err
	}
	if len(r) == 0 {
		return nil, nil
	}
	return r, nil
}

// emptyToNil maps the zero string to a NULL column value; a non-empty string
// passes through. Used for the columns whose envelope fields are omitempty
// (handler_version, error stack) so absent and empty stay distinguishable only
// as "absent".
func emptyToNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// nilToEmpty is the inverse of emptyToNil: a NULL column becomes the zero string.
func nilToEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
