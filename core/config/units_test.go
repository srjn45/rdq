// SPDX-License-Identifier: Apache-2.0

package config

import (
	"encoding/json"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestDurationUnmarshalYAML(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
		bad  bool
	}{
		{in: "1s", want: time.Second},
		{in: "500ms", want: 500 * time.Millisecond},
		{in: "10m", want: 10 * time.Minute},
		{in: "24h", want: 24 * time.Hour},
		{in: "1.5s", want: 1500 * time.Millisecond},
		{in: "60", bad: true}, // no unit — ambiguous, rejected
		{in: "banana", bad: true},
		{in: "1d", bad: true}, // days are not a Go duration unit
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			var d Duration
			err := yaml.Unmarshal([]byte(c.in), &d)
			if c.bad {
				if err == nil {
					t.Fatalf("expected error for %q, got %s", c.in, d.Std())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if d.Std() != c.want {
				t.Fatalf("got %s, want %s", d.Std(), c.want)
			}
		})
	}
}

func TestDurationJSONRoundTripMillis(t *testing.T) {
	// The admin-API wire form is integer milliseconds (design 03 §2).
	d := Duration(1500 * time.Millisecond)
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "1500" {
		t.Fatalf("got %s, want 1500", b)
	}
	var back Duration
	if err := json.Unmarshal([]byte("1500"), &back); err != nil {
		t.Fatal(err)
	}
	if back.Std() != d.Std() {
		t.Fatalf("round-trip lost value: %s != %s", back.Std(), d.Std())
	}
}

func TestSizeUnmarshalYAML(t *testing.T) {
	cases := []struct {
		in   string
		want int64
		bad  bool
	}{
		{in: "1MiB", want: 1 << 20},
		{in: "512KiB", want: 512 << 10},
		{in: "1GiB", want: 1 << 30},
		{in: "1.5MiB", want: int64(1.5 * float64(1<<20))},
		{in: "4096", want: 4096}, // bare number is bytes
		{in: "2B", want: 2},      // explicit byte suffix
		{in: "1MB", bad: true},   // decimal units unsupported
		{in: "-1MiB", bad: true}, // negative
		{in: "banana", bad: true},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			var s Size
			err := yaml.Unmarshal([]byte(c.in), &s)
			if c.bad {
				if err == nil {
					t.Fatalf("expected error for %q, got %d", c.in, s.Bytes())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if s.Bytes() != c.want {
				t.Fatalf("got %d, want %d", s.Bytes(), c.want)
			}
		})
	}
}

func TestSizeJSONBytes(t *testing.T) {
	s := Size(1 << 20)
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "1048576" {
		t.Fatalf("got %s, want 1048576", b)
	}
	var back Size
	if err := json.Unmarshal([]byte("1048576"), &back); err != nil {
		t.Fatal(err)
	}
	if back.Bytes() != s.Bytes() {
		t.Fatalf("round-trip lost value")
	}
}

func TestRateUnmarshalYAML(t *testing.T) {
	cases := []struct {
		in        string
		count     int64
		per       time.Duration
		perSecond float64
		bad       bool
	}{
		{in: "100/s", count: 100, per: time.Second, perSecond: 100},
		{in: "60/m", count: 60, per: time.Minute, perSecond: 1},
		{in: "3600/h", count: 3600, per: time.Hour, perSecond: 1},
		{in: "100", bad: true},   // no period
		{in: "100/d", bad: true}, // unsupported period
		{in: "x/s", bad: true},   // non-numeric count
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			var r Rate
			err := yaml.Unmarshal([]byte(c.in), &r)
			if c.bad {
				if err == nil {
					t.Fatalf("expected error for %q", c.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if r.Count != c.count || r.Per != c.per {
				t.Fatalf("got %d/%s, want %d/%s", r.Count, r.Per, c.count, c.per)
			}
			if r.PerSecond() != c.perSecond {
				t.Fatalf("PerSecond got %g, want %g", r.PerSecond(), c.perSecond)
			}
		})
	}
}

func TestRateJSONRoundTrip(t *testing.T) {
	r := Rate{Count: 100, Per: time.Second}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `"100/s"` {
		t.Fatalf("got %s, want \"100/s\"", b)
	}
	var back Rate
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back != r {
		t.Fatalf("round-trip lost value: %+v != %+v", back, r)
	}
}

func TestStatusMatcher(t *testing.T) {
	cases := []struct {
		in      string
		match   []int
		nomatch []int
		bad     bool
	}{
		{in: "408", match: []int{408}, nomatch: []int{409, 500}},
		{in: `"5xx"`, match: []int{500, 503, 599}, nomatch: []int{499, 200}},
		{in: `"4xx"`, match: []int{400, 429, 499}, nomatch: []int{399, 500}},
		{in: "99", bad: true},    // out of range
		{in: "600", bad: true},   // out of range
		{in: `"6xx"`, bad: true}, // no such class
		{in: `"abc"`, bad: true},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			var m StatusMatcher
			err := yaml.Unmarshal([]byte(c.in), &m)
			if c.bad {
				if err == nil {
					t.Fatalf("expected error for %q", c.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			for _, s := range c.match {
				if !m.Matches(s) {
					t.Errorf("%q should match %d", c.in, s)
				}
			}
			for _, s := range c.nomatch {
				if m.Matches(s) {
					t.Errorf("%q should not match %d", c.in, s)
				}
			}
		})
	}
}
