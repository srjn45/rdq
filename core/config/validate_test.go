// SPDX-License-Identifier: Apache-2.0

package config

import (
	"strings"
	"testing"
)

// TestValidationRules drives each field rule of design 03 §3 through Load,
// asserting that a violating document fails and (where given) that the error
// names the offending field.
func TestValidationRules(t *testing.T) {
	cases := []struct {
		name    string
		yaml    string
		wantErr bool
		errHas  string
	}{
		{
			name: "valid baseline",
			yaml: `
config_version: 1
queues:
  q:
    retry: {max_attempts: 3, backoff_multiplier: 2.0, jitter: 0.5}
    execution: {lease: 60s, handler_timeout: 30s}
`,
		},
		{
			name:    "max_attempts below 1",
			yaml:    "config_version: 1\nqueues:\n  q:\n    retry: {max_attempts: 0}\n",
			wantErr: true, errHas: "max_attempts",
		},
		{
			name:    "backoff_multiplier below 1.0",
			yaml:    "config_version: 1\nqueues:\n  q:\n    retry: {backoff_multiplier: 0.5}\n",
			wantErr: true, errHas: "backoff_multiplier",
		},
		{
			name:    "jitter above 1",
			yaml:    "config_version: 1\nqueues:\n  q:\n    retry: {jitter: 1.5}\n",
			wantErr: true, errHas: "jitter",
		},
		{
			name:    "jitter below 0",
			yaml:    "config_version: 1\nqueues:\n  q:\n    retry: {jitter: -0.1}\n",
			wantErr: true, errHas: "jitter",
		},
		{
			name:    "handler_timeout exceeds lease",
			yaml:    "config_version: 1\nqueues:\n  q:\n    execution: {lease: 30s, handler_timeout: 45s}\n",
			wantErr: true, errHas: "handler_timeout",
		},
		{
			name: "handler_timeout exceeds lease across defaults and queue",
			// lease from defaults (30s), handler_timeout from queue (45s): the
			// cross-field rule must see the merged pair.
			yaml:    "config_version: 1\ndefaults:\n  execution: {lease: 30s}\nqueues:\n  q:\n    execution: {handler_timeout: 45s}\n",
			wantErr: true, errHas: "handler_timeout",
		},
		{
			name:    "non-positive lease",
			yaml:    "config_version: 1\nqueues:\n  q:\n    execution: {lease: 0s}\n",
			wantErr: true, errHas: "lease",
		},
		{
			name:    "batch_size below 1",
			yaml:    "config_version: 1\nqueues:\n  q:\n    worker: {batch_size: 0}\n",
			wantErr: true, errHas: "batch_size",
		},
		{
			name:    "concurrency below 1",
			yaml:    "config_version: 1\nqueues:\n  q:\n    worker: {concurrency: 0}\n",
			wantErr: true, errHas: "concurrency",
		},
		{
			name:    "unknown version_mismatch value",
			yaml:    "config_version: 1\nqueues:\n  q:\n    handler: {version_mismatch: whatever}\n",
			wantErr: true, errHas: "version_mismatch",
		},
		{
			name: "valid version_mismatch",
			yaml: "config_version: 1\nqueues:\n  q:\n    handler: {version_mismatch: run-latest}\n",
		},
		{
			name:    "callback without url",
			yaml:    "config_version: 1\nqueues:\n  q:\n    callback: {protocol: http}\n",
			wantErr: true, errHas: "callback.url",
		},
		{
			name:    "callback url not absolute",
			yaml:    "config_version: 1\nqueues:\n  q:\n    callback: {url: /just/a/path}\n",
			wantErr: true, errHas: "callback.url",
		},
		{
			name:    "callback http url wrong scheme",
			yaml:    "config_version: 1\nqueues:\n  q:\n    callback: {protocol: http, url: \"ftp://host/x\"}\n",
			wantErr: true, errHas: "http or https",
		},
		{
			name:    "callback timeout exceeds handler_timeout",
			yaml:    "config_version: 1\nqueues:\n  q:\n    execution: {lease: 60s, handler_timeout: 20s}\n    callback: {url: \"https://h/x\", timeout: 30s}\n",
			wantErr: true, errHas: "callback.timeout",
		},
		{
			name:    "unknown auth type",
			yaml:    "config_version: 1\nqueues:\n  q:\n    callback: {url: \"https://h/x\", auth: {type: basic}}\n",
			wantErr: true, errHas: "auth.type",
		},
		{
			name:    "bearer auth missing secret_ref",
			yaml:    "config_version: 1\nqueues:\n  q:\n    callback: {url: \"https://h/x\", auth: {type: bearer}}\n",
			wantErr: true, errHas: "secret_ref is required",
		},
		{
			name:    "secret_ref wrong scheme",
			yaml:    "config_version: 1\nqueues:\n  q:\n    callback: {url: \"https://h/x\", auth: {type: bearer, secret_ref: \"vault:PATH\"}}\n",
			wantErr: true, errHas: "env:",
		},
		{
			name:    "secret_ref empty var name",
			yaml:    "config_version: 1\nqueues:\n  q:\n    callback: {url: \"https://h/x\", auth: {type: bearer, secret_ref: \"env:\"}}\n",
			wantErr: true, errHas: "environment variable",
		},
		{
			name: "valid callback",
			yaml: "config_version: 1\nqueues:\n  q:\n    callback: {url: \"https://h/x\", auth: {type: bearer, secret_ref: \"env:TOKEN\"}}\n",
		},
		{
			name:    "invalid queue name",
			yaml:    "config_version: 1\nqueues:\n  \"Bad Name\":\n    retry: {max_attempts: 3}\n",
			wantErr: true, errHas: "queue",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Load([]byte(c.yaml))
			if c.wantErr != (err != nil) {
				t.Fatalf("Load err=%v, wantErr=%v", err, c.wantErr)
			}
			if c.wantErr && c.errHas != "" && !strings.Contains(err.Error(), c.errHas) {
				t.Fatalf("error %q does not mention %q", err, c.errHas)
			}
		})
	}
}

// TestInvalidDefaultsRejected ensures a bad value in the defaults block fails
// even when no queue references it.
func TestInvalidDefaultsRejected(t *testing.T) {
	yaml := "config_version: 1\ndefaults:\n  retry: {jitter: 5.0}\n"
	if _, err := Load([]byte(yaml)); err == nil {
		t.Fatal("expected an error for an invalid defaults value")
	}
}

// TestRateLimitPresenceOptional confirms an omitted rate_limit is legal
// (unlimited) while a present zero-count rate is rejected.
func TestRateLimitPresenceOptional(t *testing.T) {
	if _, err := Load([]byte("config_version: 1\nqueues:\n  q:\n    worker: {concurrency: 4}\n")); err != nil {
		t.Fatalf("omitted rate_limit should be legal: %v", err)
	}
	if _, err := Load([]byte("config_version: 1\nqueues:\n  q:\n    worker: {rate_limit: \"0/s\"}\n")); err == nil {
		t.Fatal("zero-count rate_limit should be rejected")
	}
}
