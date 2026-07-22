// SPDX-License-Identifier: Apache-2.0

package envelope

// Canonical JSON codec for the wire envelope (design 01 §1). The canonical form
// is compact UTF-8 JSON with snake_case keys, RFC-3339 UTC millisecond
// timestamps, integer-millisecond durations, base64 payloads, and ULID ids.
// HTML escaping is disabled so the bytes are identical across languages — these
// fixtures are the cross-language contract replayed by Postgres (M2) and Java
// (M7). Marshal/Unmarshal go through a private wire mirror of Envelope so the
// struct in envelope.go keeps plain time.Time fields while the wire form stays
// canonical.

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

// rfc3339Millis formats a UTC timestamp with exactly three fractional digits
// (design 01 §1: "2026-07-20T14:03:22.117Z"). The Z07:00 slot renders "Z" for
// UTC times, which the codec always produces.
const rfc3339Millis = "2006-01-02T15:04:05.000Z07:00"

// Time is a time.Time that marshals to RFC-3339 UTC with millisecond precision.
// Trailing zeros are never trimmed, so ".200" stays ".200" — the property plain
// time.Time marshaling lacks and the reason this type exists.
type Time time.Time

// MarshalJSON renders t as a canonical millisecond RFC-3339 UTC string.
func (t Time) MarshalJSON() ([]byte, error) {
	b := make([]byte, 0, len(rfc3339Millis)+2)
	b = append(b, '"')
	b = time.Time(t).UTC().AppendFormat(b, rfc3339Millis)
	b = append(b, '"')
	return b, nil
}

// UnmarshalJSON parses an RFC-3339 timestamp (any precision) into UTC.
func (t *Time) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	parsed, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return fmt.Errorf("envelope: invalid timestamp %q: %w", s, err)
	}
	*t = Time(parsed.UTC())
	return nil
}

// Duration is a time.Duration encoded as integer milliseconds (design 01 §1).
type Duration time.Duration

// MarshalJSON renders d as a bare integer count of milliseconds.
func (d Duration) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatInt(int64(time.Duration(d)/time.Millisecond), 10)), nil
}

// UnmarshalJSON parses an integer count of milliseconds into d.
func (d *Duration) UnmarshalJSON(data []byte) error {
	ms, err := strconv.ParseInt(string(bytes.TrimSpace(data)), 10, 64)
	if err != nil {
		return fmt.Errorf("envelope: invalid duration %q: %w", data, err)
	}
	*d = Duration(time.Duration(ms) * time.Millisecond)
	return nil
}

// --- ULID (design 01 §1: lexicographically sortable id, no coordination) ---

// ulidEnc is Crockford's base32 alphabet (no I, L, O, U).
const ulidEnc = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// ulidDec maps an ASCII byte to its 5-bit value, or 0xFF when not a canonical
// (uppercase) Crockford digit.
var ulidDec [256]byte

func init() {
	for i := range ulidDec {
		ulidDec[i] = 0xFF
	}
	for i := 0; i < len(ulidEnc); i++ {
		ulidDec[ulidEnc[i]] = byte(i)
	}
}

// ULID is a 128-bit identifier: 48-bit big-endian millisecond timestamp followed
// by 80 bits of randomness, rendered as 26 Crockford base32 characters.
type ULID [16]byte

// NewULID generates a ULID stamped with the current time.
func NewULID() (ULID, error) { return NewULIDAt(time.Now()) }

// NewULIDAt generates a ULID whose timestamp component is t (millisecond
// precision) and whose low 80 bits come from crypto/rand.
func NewULIDAt(t time.Time) (ULID, error) {
	var id ULID
	ms := uint64(t.UnixMilli())
	id[0] = byte(ms >> 40)
	id[1] = byte(ms >> 32)
	id[2] = byte(ms >> 24)
	id[3] = byte(ms >> 16)
	id[4] = byte(ms >> 8)
	id[5] = byte(ms)
	if _, err := rand.Read(id[6:]); err != nil {
		return ULID{}, err
	}
	return id, nil
}

// ParseULID decodes the 26-character canonical form, rejecting bad length,
// non-alphabet characters, and timestamp overflow.
func ParseULID(s string) (ULID, error) {
	var id ULID
	if len(s) != 26 {
		return id, fmt.Errorf("envelope: invalid ULID %q: want 26 chars, got %d", s, len(s))
	}
	v := []byte(s)
	for _, c := range v {
		if ulidDec[c] == 0xFF {
			return id, fmt.Errorf("envelope: invalid ULID %q: bad character %q", s, string(c))
		}
	}
	// 26 base32 chars carry 130 bits; the top 2 must be zero for a 128-bit id,
	// so the first character's value cannot exceed 7.
	if ulidDec[v[0]] > 7 {
		return id, fmt.Errorf("envelope: invalid ULID %q: timestamp overflow", s)
	}
	id[0] = ulidDec[v[0]]<<5 | ulidDec[v[1]]
	id[1] = ulidDec[v[2]]<<3 | ulidDec[v[3]]>>2
	id[2] = ulidDec[v[3]]<<6 | ulidDec[v[4]]<<1 | ulidDec[v[5]]>>4
	id[3] = ulidDec[v[5]]<<4 | ulidDec[v[6]]>>1
	id[4] = ulidDec[v[6]]<<7 | ulidDec[v[7]]<<2 | ulidDec[v[8]]>>3
	id[5] = ulidDec[v[8]]<<5 | ulidDec[v[9]]
	id[6] = ulidDec[v[10]]<<3 | ulidDec[v[11]]>>2
	id[7] = ulidDec[v[11]]<<6 | ulidDec[v[12]]<<1 | ulidDec[v[13]]>>4
	id[8] = ulidDec[v[13]]<<4 | ulidDec[v[14]]>>1
	id[9] = ulidDec[v[14]]<<7 | ulidDec[v[15]]<<2 | ulidDec[v[16]]>>3
	id[10] = ulidDec[v[16]]<<5 | ulidDec[v[17]]
	id[11] = ulidDec[v[18]]<<3 | ulidDec[v[19]]>>2
	id[12] = ulidDec[v[19]]<<6 | ulidDec[v[20]]<<1 | ulidDec[v[21]]>>4
	id[13] = ulidDec[v[21]]<<4 | ulidDec[v[22]]>>1
	id[14] = ulidDec[v[22]]<<7 | ulidDec[v[23]]<<2 | ulidDec[v[24]]>>3
	id[15] = ulidDec[v[24]]<<5 | ulidDec[v[25]]
	return id, nil
}

// String renders the 26-character canonical form.
func (id ULID) String() string {
	dst := make([]byte, 26)
	dst[0] = ulidEnc[(id[0]&224)>>5]
	dst[1] = ulidEnc[id[0]&31]
	dst[2] = ulidEnc[(id[1]&248)>>3]
	dst[3] = ulidEnc[((id[1]&7)<<2)|((id[2]&192)>>6)]
	dst[4] = ulidEnc[(id[2]&62)>>1]
	dst[5] = ulidEnc[((id[2]&1)<<4)|((id[3]&240)>>4)]
	dst[6] = ulidEnc[((id[3]&15)<<1)|((id[4]&128)>>7)]
	dst[7] = ulidEnc[(id[4]&124)>>2]
	dst[8] = ulidEnc[((id[4]&3)<<3)|((id[5]&224)>>5)]
	dst[9] = ulidEnc[id[5]&31]
	dst[10] = ulidEnc[(id[6]&248)>>3]
	dst[11] = ulidEnc[((id[6]&7)<<2)|((id[7]&192)>>6)]
	dst[12] = ulidEnc[(id[7]&62)>>1]
	dst[13] = ulidEnc[((id[7]&1)<<4)|((id[8]&240)>>4)]
	dst[14] = ulidEnc[((id[8]&15)<<1)|((id[9]&128)>>7)]
	dst[15] = ulidEnc[(id[9]&124)>>2]
	dst[16] = ulidEnc[((id[9]&3)<<3)|((id[10]&224)>>5)]
	dst[17] = ulidEnc[id[10]&31]
	dst[18] = ulidEnc[(id[11]&248)>>3]
	dst[19] = ulidEnc[((id[11]&7)<<2)|((id[12]&192)>>6)]
	dst[20] = ulidEnc[(id[12]&62)>>1]
	dst[21] = ulidEnc[((id[12]&1)<<4)|((id[13]&240)>>4)]
	dst[22] = ulidEnc[((id[13]&15)<<1)|((id[14]&128)>>7)]
	dst[23] = ulidEnc[(id[14]&124)>>2]
	dst[24] = ulidEnc[((id[14]&3)<<3)|((id[15]&224)>>5)]
	dst[25] = ulidEnc[id[15]&31]
	return string(dst)
}

// Time returns the millisecond timestamp encoded in the id's high 48 bits.
func (id ULID) Time() time.Time {
	ms := int64(id[0])<<40 | int64(id[1])<<32 | int64(id[2])<<24 |
		int64(id[3])<<16 | int64(id[4])<<8 | int64(id[5])
	return time.UnixMilli(ms).UTC()
}

// MarshalText renders the canonical string form (used as a JSON string).
func (id ULID) MarshalText() ([]byte, error) { return []byte(id.String()), nil }

// UnmarshalText parses the canonical string form.
func (id *ULID) UnmarshalText(text []byte) error {
	parsed, err := ParseULID(string(text))
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

// --- wire mirror + Marshal/Unmarshal ---

// wireEnvelope mirrors Envelope field-for-field but carries canonical Time
// values. Keep the fields and json tags in lockstep with envelope.go.
type wireEnvelope struct {
	EnvelopeVersion int    `json:"envelope_version"`
	ID              string `json:"id"`
	Queue           string `json:"queue"`
	HandlerRef      string `json:"handler_ref"`
	HandlerVersion  string `json:"handler_version,omitempty"`

	Payload            []byte  `json:"payload"`
	PayloadContentType string  `json:"payload_content_type"`
	PayloadRef         *string `json:"payload_ref,omitempty"`

	Headers map[string]string `json:"headers,omitempty"`

	Status       Status `json:"status"`
	AttemptCount int    `json:"attempt_count"`
	RedriveCount int    `json:"redrive_count"`

	NextAttemptAt  *Time `json:"next_attempt_at"`
	LeaseExpiresAt *Time `json:"lease_expires_at"`
	CreatedAt      Time  `json:"created_at"`

	Attempts []wireAttempt `json:"attempts,omitempty"`

	// Residual carries unknown top-level fields; json:"-" so the residual
	// merge (MarshalJSON) is the only path that emits them.
	Residual map[string]json.RawMessage `json:"-"`
}

// wireAttempt mirrors Attempt with a canonical Time.
type wireAttempt struct {
	AttemptNo  int     `json:"attempt_no"`
	StartedAt  Time    `json:"started_at"`
	FinishedAt *Time   `json:"finished_at"`
	Outcome    Outcome `json:"outcome"`
	Error      *Error  `json:"error,omitempty"`

	// Residual carries unknown per-attempt fields (see wireEnvelope.Residual).
	Residual map[string]json.RawMessage `json:"-"`
}

func wireTimePtr(t *time.Time) *Time {
	if t == nil {
		return nil
	}
	w := Time(*t)
	return &w
}

func timePtr(t *Time) *time.Time {
	if t == nil {
		return nil
	}
	tt := time.Time(*t)
	return &tt
}

func toWire(e *Envelope) *wireEnvelope {
	w := &wireEnvelope{
		EnvelopeVersion:    e.EnvelopeVersion,
		ID:                 e.ID,
		Queue:              e.Queue,
		HandlerRef:         e.HandlerRef,
		HandlerVersion:     e.HandlerVersion,
		Payload:            e.Payload,
		PayloadContentType: e.PayloadContentType,
		PayloadRef:         e.PayloadRef,
		Headers:            e.Headers,
		Status:             e.Status,
		AttemptCount:       e.AttemptCount,
		RedriveCount:       e.RedriveCount,
		NextAttemptAt:      wireTimePtr(e.NextAttemptAt),
		LeaseExpiresAt:     wireTimePtr(e.LeaseExpiresAt),
		CreatedAt:          Time(e.CreatedAt),
		Residual:           e.Residual,
	}
	if e.Attempts != nil {
		w.Attempts = make([]wireAttempt, len(e.Attempts))
		for i, a := range e.Attempts {
			w.Attempts[i] = wireAttempt{
				AttemptNo:  a.AttemptNo,
				StartedAt:  Time(a.StartedAt),
				FinishedAt: wireTimePtr(a.FinishedAt),
				Outcome:    a.Outcome,
				Error:      a.Error,
				Residual:   a.Residual,
			}
		}
	}
	return w
}

func (w *wireEnvelope) toEnvelope() Envelope {
	e := Envelope{
		EnvelopeVersion:    w.EnvelopeVersion,
		ID:                 w.ID,
		Queue:              w.Queue,
		HandlerRef:         w.HandlerRef,
		HandlerVersion:     w.HandlerVersion,
		Payload:            w.Payload,
		PayloadContentType: w.PayloadContentType,
		PayloadRef:         w.PayloadRef,
		Headers:            w.Headers,
		Status:             w.Status,
		AttemptCount:       w.AttemptCount,
		RedriveCount:       w.RedriveCount,
		NextAttemptAt:      timePtr(w.NextAttemptAt),
		LeaseExpiresAt:     timePtr(w.LeaseExpiresAt),
		CreatedAt:          time.Time(w.CreatedAt),
		Residual:           w.Residual,
	}
	if w.Attempts != nil {
		e.Attempts = make([]Attempt, len(w.Attempts))
		for i, a := range w.Attempts {
			e.Attempts[i] = Attempt{
				AttemptNo:  a.AttemptNo,
				StartedAt:  time.Time(a.StartedAt),
				FinishedAt: timePtr(a.FinishedAt),
				Outcome:    a.Outcome,
				Error:      a.Error,
				Residual:   a.Residual,
			}
		}
	}
	return e
}

// --- unknown-field (residual) preservation (design 01 §5, rule 1) ---
//
// Fields present on the wire but absent from the wire structs are captured into
// a Residual map on decode and re-emitted on encode, so a task written by a
// newer envelope_version round-trips losslessly through an older reader. Both
// the top-level object and each attempt object carry their own residual.
// Residual keys are emitted after all known fields, sorted, so the canonical
// bytes stay deterministic across languages (like the sorted keys of a map).

// envelopeKnownKeys and attemptKnownKeys are the wire json field names, derived
// once from the struct tags so adding a wire field never requires updating a
// hand-maintained list.
var (
	envelopeKnownKeys = jsonFieldNames(reflect.TypeOf(wireEnvelope{}))
	attemptKnownKeys  = jsonFieldNames(reflect.TypeOf(wireAttempt{}))
)

// jsonFieldNames returns the set of wire json field names of struct type t,
// skipping fields tagged json:"-" (the Residual field itself).
func jsonFieldNames(t reflect.Type) map[string]struct{} {
	names := make(map[string]struct{}, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		tag := t.Field(i).Tag.Get("json")
		if tag == "" || tag == "-" {
			continue
		}
		if comma := strings.IndexByte(tag, ','); comma >= 0 {
			tag = tag[:comma]
		}
		if tag != "" {
			names[tag] = struct{}{}
		}
	}
	return names
}

// marshalNoEscape encodes v as compact JSON with HTML escaping disabled and no
// trailing newline — the same canonical settings Marshal uses.
func marshalNoEscape(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// captureResidual returns the object's fields whose keys are not in known, or
// nil when none remain (so envelopes without unknown fields keep a nil map).
func captureResidual(data []byte, known map[string]struct{}) (map[string]json.RawMessage, error) {
	var all map[string]json.RawMessage
	if err := json.Unmarshal(data, &all); err != nil {
		return nil, err
	}
	for k := range all {
		if _, ok := known[k]; ok {
			delete(all, k)
		}
	}
	if len(all) == 0 {
		return nil, nil
	}
	return all, nil
}

// appendResidual writes known's fields followed by residual's, the latter sorted
// by key so the canonical output is deterministic. known must be a JSON object.
func appendResidual(known []byte, residual map[string]json.RawMessage) ([]byte, error) {
	if len(residual) == 0 {
		return known, nil
	}
	keys := make([]string, 0, len(residual))
	for k := range residual {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]byte, 0, len(known)+len(keys)*16)
	out = append(out, known[:len(known)-1]...) // drop the closing '}'
	first := len(known) == 2                   // known == "{}" → first pair takes no comma
	for _, k := range keys {
		if !first {
			out = append(out, ',')
		}
		first = false
		key, err := marshalNoEscape(k)
		if err != nil {
			return nil, err
		}
		out = append(out, key...)
		out = append(out, ':')
		// Compact (not re-marshal) the value so its bytes — and thus any
		// escaping the newer writer chose — survive verbatim.
		var val bytes.Buffer
		if err := json.Compact(&val, residual[k]); err != nil {
			return nil, fmt.Errorf("envelope: invalid residual value for %q: %w", k, err)
		}
		out = append(out, val.Bytes()...)
	}
	out = append(out, '}')
	return out, nil
}

// MarshalJSON encodes the known wire fields, then appends any residual unknown
// top-level fields.
func (w wireEnvelope) MarshalJSON() ([]byte, error) {
	type alias wireEnvelope // strips the methods → plain reflection marshal
	known, err := marshalNoEscape(alias(w))
	if err != nil {
		return nil, err
	}
	return appendResidual(known, w.Residual)
}

// UnmarshalJSON decodes the known wire fields and captures every other
// top-level field into Residual.
func (w *wireEnvelope) UnmarshalJSON(data []byte) error {
	type alias wireEnvelope
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	*w = wireEnvelope(a)
	residual, err := captureResidual(data, envelopeKnownKeys)
	if err != nil {
		return err
	}
	w.Residual = residual
	return nil
}

// MarshalJSON mirrors wireEnvelope.MarshalJSON for a single attempt object.
func (w wireAttempt) MarshalJSON() ([]byte, error) {
	type alias wireAttempt
	known, err := marshalNoEscape(alias(w))
	if err != nil {
		return nil, err
	}
	return appendResidual(known, w.Residual)
}

// UnmarshalJSON mirrors wireEnvelope.UnmarshalJSON for a single attempt object.
func (w *wireAttempt) UnmarshalJSON(data []byte) error {
	type alias wireAttempt
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	*w = wireAttempt(a)
	residual, err := captureResidual(data, attemptKnownKeys)
	if err != nil {
		return err
	}
	w.Residual = residual
	return nil
}

// Marshal encodes e in the canonical wire form (design 01 §1): compact JSON,
// no HTML escaping, no trailing newline.
func Marshal(e *Envelope) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(toWire(e)); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// Unmarshal decodes canonical wire JSON into a new Envelope.
func Unmarshal(data []byte) (*Envelope, error) {
	var w wireEnvelope
	if err := json.Unmarshal(data, &w); err != nil {
		return nil, err
	}
	e := w.toEnvelope()
	return &e, nil
}
