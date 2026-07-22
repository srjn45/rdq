// SPDX-License-Identifier: Apache-2.0

package audit_test

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/srjn45/rdq/core/audit"
)

func TestLogSinkShape(t *testing.T) {
	var buf bytes.Buffer
	sink := audit.NewLogSink(&buf)

	ts := time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)
	r := audit.Record{
		Timestamp:    ts,
		Principal:    "ops-bot",
		Action:       audit.ActionRedrive,
		Queue:        "payments",
		Selector:     "filter:{error_type:timeout}",
		Count:        5,
		Outcome:      audit.OutcomeSuccess,
		ErrorMessage: "",
	}

	if err := sink.Emit(r); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("output is not valid JSON: %v\nraw: %s", err, buf.String())
	}

	checks := map[string]string{
		"msg":       "audit.event",
		"principal": "ops-bot",
		"action":    "redrive",
		"queue":     "payments",
		"selector":  "filter:{error_type:timeout}",
		"outcome":   "success",
	}
	for key, want := range checks {
		got, ok := got[key]
		if !ok {
			t.Errorf("field %q missing from output", key)
			continue
		}
		if got != want {
			t.Errorf("field %q: got %q, want %q", key, got, want)
		}
	}

	// count is numeric
	if v, ok := got["count"].(float64); !ok || v != 5 {
		t.Errorf("field count: got %v, want 5", got["count"])
	}

	// timestamp present and non-empty
	if ts, ok := got["timestamp"].(string); !ok || ts == "" {
		t.Errorf("field timestamp missing or empty")
	}
}

func TestLogSinkDiscard(t *testing.T) {
	sink := audit.Discard()
	if err := sink.Emit(audit.Record{Action: audit.ActionPurge}); err != nil {
		t.Fatalf("Discard().Emit: %v", err)
	}
}

func TestLogSinkFailureRecord(t *testing.T) {
	var buf bytes.Buffer
	sink := audit.NewLogSink(&buf)

	r := audit.Record{
		Timestamp:    time.Now().UTC(),
		Principal:    "anonymous",
		Action:       audit.ActionPurge,
		Queue:        "q1",
		Selector:     "all",
		Count:        -1,
		Outcome:      audit.OutcomeFailure,
		ErrorMessage: "storage unavailable",
	}
	if err := sink.Emit(r); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	if got["outcome"] != "failure" {
		t.Errorf("outcome: got %v, want failure", got["outcome"])
	}
	if got["error"] != "storage unavailable" {
		t.Errorf("error: got %v, want 'storage unavailable'", got["error"])
	}
}

func TestLogSinkAllActions(t *testing.T) {
	actions := []audit.Action{
		audit.ActionRedrive,
		audit.ActionPurge,
		audit.ActionPause,
		audit.ActionResume,
		audit.ActionConfigWrite,
	}
	for _, action := range actions {
		var buf bytes.Buffer
		sink := audit.NewLogSink(&buf)
		if err := sink.Emit(audit.Record{Action: action, Outcome: audit.OutcomeSuccess}); err != nil {
			t.Errorf("Emit(%s): %v", action, err)
		}
		var got map[string]any
		if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
			t.Errorf("action %s: invalid JSON: %v", action, err)
		}
		if got["action"] != string(action) {
			t.Errorf("action %s: got %v in output", action, got["action"])
		}
	}
}
