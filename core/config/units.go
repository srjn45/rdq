// SPDX-License-Identifier: Apache-2.0

package config

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Human-friendly scalar types for the queue-config schema (design 03 §2). YAML
// is the human-facing form — durations carry units (`500ms`, `1s`, `10m`),
// sizes carry binary units (`KiB`, `MiB`), and a rate reads as `100/s`. The
// admin API speaks the same schema as JSON, where the wire form is machine
// integers (design 03 §2): durations are integer milliseconds and sizes are
// integer bytes. Each type therefore decodes the human YAML form and the
// integer JSON form, so one config model serves both the boot YAML file and the
// hot admin API.

// Duration is a time.Duration that reads a human unit string from YAML
// (`1s`, `500ms`, `10m`, `24h`) and integer milliseconds from JSON — the wire
// form the admin API uses (design 03 §2).
type Duration time.Duration

// Std returns the underlying time.Duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

// UnmarshalYAML parses a Go-style duration string such as "1s" or "500ms".
// A bare number (no unit) is rejected: in a human file "60" is ambiguous, so a
// unit is required (design 03 §2).
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err != nil {
		return fmt.Errorf("duration must be a units string like \"1s\": %w", err)
	}
	parsed, err := time.ParseDuration(strings.TrimSpace(s))
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// MarshalJSON emits integer milliseconds, the admin-API wire form (design 03 §2).
func (d Duration) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatInt(int64(time.Duration(d)/time.Millisecond), 10)), nil
}

// UnmarshalJSON reads integer milliseconds, the admin-API wire form.
func (d *Duration) UnmarshalJSON(data []byte) error {
	var ms int64
	if err := json.Unmarshal(data, &ms); err != nil {
		return fmt.Errorf("duration (ms) must be an integer: %w", err)
	}
	*d = Duration(time.Duration(ms) * time.Millisecond)
	return nil
}

// sizeUnits are the binary size suffixes accepted in YAML, longest-matching
// first so "KiB" wins over the bare "B" suffix it also ends with.
var sizeUnits = []struct {
	suffix string
	mult   int64
}{
	{"KiB", 1 << 10},
	{"MiB", 1 << 20},
	{"GiB", 1 << 30},
	{"TiB", 1 << 40},
	{"B", 1},
}

// Size is a byte count that reads a binary-unit string from YAML (`1MiB`,
// `512KiB`, `4096`) and an integer byte count from JSON (design 03 §2).
type Size int64

// Bytes returns the size as a plain byte count.
func (s Size) Bytes() int64 { return int64(s) }

// UnmarshalYAML parses a size with an optional binary unit suffix. A fractional
// mantissa is allowed ("1.5MiB"); a bare number is interpreted as bytes.
func (s *Size) UnmarshalYAML(node *yaml.Node) error {
	var raw string
	if err := node.Decode(&raw); err != nil {
		return fmt.Errorf("size must be a string like \"1MiB\" or a byte count: %w", err)
	}
	n, err := parseSize(strings.TrimSpace(raw))
	if err != nil {
		return err
	}
	*s = Size(n)
	return nil
}

// parseSize converts a human size string to a byte count.
func parseSize(raw string) (int64, error) {
	if raw == "" {
		return 0, fmt.Errorf("size must not be empty")
	}
	for _, u := range sizeUnits {
		if !strings.HasSuffix(raw, u.suffix) {
			continue
		}
		num := strings.TrimSpace(strings.TrimSuffix(raw, u.suffix))
		f, err := strconv.ParseFloat(num, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid size %q: %w", raw, err)
		}
		if f < 0 {
			return 0, fmt.Errorf("invalid size %q: must not be negative", raw)
		}
		return int64(f * float64(u.mult)), nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q: want a byte count or a KiB/MiB/GiB value", raw)
	}
	return n, nil
}

// MarshalJSON emits an integer byte count, the admin-API wire form.
func (s Size) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatInt(int64(s), 10)), nil
}

// UnmarshalJSON reads an integer byte count, the admin-API wire form.
func (s *Size) UnmarshalJSON(data []byte) error {
	var n int64
	if err := json.Unmarshal(data, &n); err != nil {
		return fmt.Errorf("size (bytes) must be an integer: %w", err)
	}
	*s = Size(n)
	return nil
}

// ratePeriods maps a rate-string period suffix to its duration.
var ratePeriods = map[string]time.Duration{
	"s": time.Second,
	"m": time.Minute,
	"h": time.Hour,
}

// Rate is a token-bucket rate written as `count/period` in YAML — `100/s`,
// `60/m` (design 03 §2, the per-instance `worker.rate_limit`, G12). Omitting the
// field entirely means unlimited; a present rate always has a positive count.
type Rate struct {
	Count int64
	Per   time.Duration
}

// PerSecond returns the rate as a tokens-per-second value.
func (r Rate) PerSecond() float64 {
	if r.Per <= 0 {
		return 0
	}
	return float64(r.Count) / r.Per.Seconds()
}

// String renders the canonical `count/period` form.
func (r Rate) String() string {
	for suffix, d := range ratePeriods {
		if d == r.Per {
			return strconv.FormatInt(r.Count, 10) + "/" + suffix
		}
	}
	return fmt.Sprintf("%d/%s", r.Count, r.Per)
}

// UnmarshalYAML parses a `count/period` rate string.
func (r *Rate) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err != nil {
		return fmt.Errorf("rate must be a string like \"100/s\": %w", err)
	}
	parsed, err := parseRate(strings.TrimSpace(s))
	if err != nil {
		return err
	}
	*r = parsed
	return nil
}

// parseRate converts a `count/period` string to a Rate.
func parseRate(s string) (Rate, error) {
	slash := strings.IndexByte(s, '/')
	if slash < 0 {
		return Rate{}, fmt.Errorf("invalid rate %q: want count/period like \"100/s\"", s)
	}
	count, err := strconv.ParseInt(strings.TrimSpace(s[:slash]), 10, 64)
	if err != nil {
		return Rate{}, fmt.Errorf("invalid rate %q: count must be an integer", s)
	}
	period, ok := ratePeriods[strings.TrimSpace(s[slash+1:])]
	if !ok {
		return Rate{}, fmt.Errorf("invalid rate %q: period must be one of s, m, h", s)
	}
	return Rate{Count: count, Per: period}, nil
}

// MarshalJSON emits the canonical `count/period` string.
func (r Rate) MarshalJSON() ([]byte, error) { return json.Marshal(r.String()) }

// UnmarshalJSON reads the canonical `count/period` string.
func (r *Rate) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("rate must be a string like \"100/s\": %w", err)
	}
	parsed, err := parseRate(strings.TrimSpace(s))
	if err != nil {
		return err
	}
	*r = parsed
	return nil
}

// StatusMatcher matches an HTTP callback response status in a response_mapping
// list (design 03 §2): either an exact code (`408`, `429`) or a class glob
// (`4xx`, `5xx`). Exactly one of Code / Class is set.
type StatusMatcher struct {
	Code  int    // exact status when > 0
	Class string // "1xx".."5xx" class when non-empty
}

// Matches reports whether an HTTP status code satisfies this matcher.
func (m StatusMatcher) Matches(status int) bool {
	if m.Code > 0 {
		return status == m.Code
	}
	if len(m.Class) == 3 {
		return status >= int(m.Class[0]-'0')*100 && status < (int(m.Class[0]-'0')+1)*100
	}
	return false
}

// String renders the code or class.
func (m StatusMatcher) String() string {
	if m.Code > 0 {
		return strconv.Itoa(m.Code)
	}
	return m.Class
}

// UnmarshalYAML accepts either an integer status code or a class string.
func (m *StatusMatcher) UnmarshalYAML(node *yaml.Node) error {
	var code int
	if err := node.Decode(&code); err == nil {
		if code < 100 || code > 599 {
			return fmt.Errorf("invalid status code %d: must be 100..599", code)
		}
		m.Code, m.Class = code, ""
		return nil
	}
	var class string
	if err := node.Decode(&class); err != nil {
		return fmt.Errorf("status must be a code (408) or a class (\"5xx\")")
	}
	if err := validStatusClass(class); err != nil {
		return err
	}
	m.Code, m.Class = 0, class
	return nil
}

// validStatusClass checks a status-class token like "5xx".
func validStatusClass(class string) error {
	if len(class) == 3 && class[0] >= '1' && class[0] <= '5' && class[1] == 'x' && class[2] == 'x' {
		return nil
	}
	return fmt.Errorf("invalid status class %q: want one of 1xx..5xx", class)
}

// MarshalJSON emits an integer code or a class string.
func (m StatusMatcher) MarshalJSON() ([]byte, error) {
	if m.Code > 0 {
		return json.Marshal(m.Code)
	}
	return json.Marshal(m.Class)
}

// UnmarshalJSON accepts either an integer status code or a class string — the
// same two forms as UnmarshalYAML and MarshalJSON — so the admin API can
// round-trip response_mapping entries (design 03 §2).
func (m *StatusMatcher) UnmarshalJSON(data []byte) error {
	var code int
	if err := json.Unmarshal(data, &code); err == nil {
		if code < 100 || code > 599 {
			return fmt.Errorf("invalid status code %d: must be 100..599", code)
		}
		m.Code, m.Class = code, ""
		return nil
	}
	var class string
	if err := json.Unmarshal(data, &class); err != nil {
		return fmt.Errorf("status must be a code (408) or a class (\"5xx\")")
	}
	if err := validStatusClass(class); err != nil {
		return err
	}
	m.Code, m.Class = 0, class
	return nil
}
