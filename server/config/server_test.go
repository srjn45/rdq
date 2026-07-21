// SPDX-License-Identifier: Apache-2.0

package config

import (
	"strings"
	"testing"

	coreconfig "github.com/srjn45/rdq/core/config"
)

func strptr(s string) *string { return &s }

// TestAllowlistMatching: host/scheme/path-prefix matching, with deny-by-default
// for anything not explicitly listed (SSRF boundary).
func TestAllowlistMatching(t *testing.T) {
	al, err := ParseAllowlist([]string{
		"https://payments.internal/hooks",
		"receipts.internal", // bare host: any scheme/path
		"https://ledger.internal:8443",
	})
	if err != nil {
		t.Fatalf("ParseAllowlist: %v", err)
	}
	cases := []struct {
		url  string
		want bool
	}{
		{"https://payments.internal/hooks", true},
		{"https://payments.internal/hooks/charge", true},    // under the prefix
		{"https://payments.internal/other", false},          // wrong path prefix
		{"http://payments.internal/hooks", false},           // wrong scheme (https pinned)
		{"https://payments.internal.evil.com/hooks", false}, // host is exact, no suffix trick
		{"http://receipts.internal/anything", true},         // bare host: any scheme/path
		{"https://receipts.internal/x", true},
		{"https://ledger.internal:8443/cb", true},  // host:port matches
		{"https://ledger.internal/cb", false},      // missing port ≠ listed host:port
		{"https://unlisted.internal/hooks", false}, // deny by default
		{"://bad", false},                          // unparseable/host-less
	}
	for _, tc := range cases {
		if got := al.Allows(tc.url); got != tc.want {
			t.Errorf("Allows(%q) = %v, want %v", tc.url, got, tc.want)
		}
	}
}

// TestEmptyAllowlistDeniesAll: with no entries, no callback URL is permitted.
func TestEmptyAllowlistDeniesAll(t *testing.T) {
	al, err := ParseAllowlist(nil)
	if err != nil {
		t.Fatal(err)
	}
	if al.Allows("https://anything.internal/hook") {
		t.Error("empty allowlist must deny all callback URLs")
	}
}

// TestParseAllowlistRejectsHostless: a malformed entry fails fast at load.
func TestParseAllowlistRejectsHostless(t *testing.T) {
	for _, bad := range []string{"", "   ", "https:///nohost", "/just/a/path"} {
		if _, err := ParseAllowlist([]string{bad}); err == nil {
			t.Errorf("ParseAllowlist(%q) should error", bad)
		}
	}
}

// TestResolveSecretRef: env: deref succeeds for a set var and fails fast for
// every invalid case (design 03 §3).
func TestResolveSecretRef(t *testing.T) {
	env := func(m map[string]string) func(string) (string, bool) {
		return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
	}

	got, err := ResolveSecretRef("env:PAYMENTS_CB_TOKEN", env(map[string]string{"PAYMENTS_CB_TOKEN": "s3cr3t"}))
	if err != nil || string(got) != "s3cr3t" {
		t.Fatalf("ResolveSecretRef good = (%q, %v), want (s3cr3t, nil)", got, err)
	}

	bad := []struct {
		name string
		ref  string
		envm map[string]string
		want string
	}{
		{"unset", "env:MISSING", map[string]string{}, "is not set"},
		{"empty value", "env:BLANK", map[string]string{"BLANK": ""}, "is empty"},
		{"no var name", "env:", map[string]string{}, "names no environment variable"},
		{"wrong scheme", "vault:secret/x", map[string]string{}, "env: scheme"},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ResolveSecretRef(tc.ref, env(tc.envm)); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ResolveSecretRef(%q) err = %v, want containing %q", tc.ref, err, tc.want)
			}
		})
	}
}

// TestValidateCallbacksRejectsOffAllowlist: a callback URL off the allowlist is
// REJECTED AT CONFIG LOAD (the T5.6 acceptance), and a secret_ref that does not
// resolve fails the same way.
func TestValidateCallbacksRejectsOffAllowlist(t *testing.T) {
	sc := &ServerConfig{CallbackAllowlist: []string{"https://payments.internal"}}
	env := func(k string) (string, bool) {
		if k == "PAY_TOKEN" {
			return "tok", true
		}
		return "", false
	}

	mkQueue := func(url string, secretRef *string) map[string]*coreconfig.QueueConfig {
		cb := &coreconfig.CallbackConfig{URL: strptr(url)}
		if secretRef != nil {
			bearer := coreconfig.AuthBearer
			cb.Auth = &coreconfig.CallbackAuth{Type: &bearer, SecretRef: secretRef}
		}
		return map[string]*coreconfig.QueueConfig{"payments.charge": {Callback: cb}}
	}

	// On-allowlist URL with a resolvable secret → OK.
	if err := sc.ValidateCallbacks(mkQueue("https://payments.internal/hooks", strptr("env:PAY_TOKEN")), env); err != nil {
		t.Errorf("on-allowlist config should validate, got %v", err)
	}

	// Off-allowlist URL → rejected at load.
	err := sc.ValidateCallbacks(mkQueue("https://evil.internal/hooks", nil), env)
	if err == nil || !strings.Contains(err.Error(), "not on the callback allowlist") {
		t.Errorf("off-allowlist url err = %v, want allowlist rejection", err)
	}

	// On-allowlist URL but unresolvable secret_ref → rejected at load.
	err = sc.ValidateCallbacks(mkQueue("https://payments.internal/hooks", strptr("env:MISSING")), env)
	if err == nil || !strings.Contains(err.Error(), "is not set") {
		t.Errorf("unresolvable secret err = %v, want secret resolution failure", err)
	}

	// A queue with no callback is not subject to the allowlist.
	noCallback := map[string]*coreconfig.QueueConfig{"plain": {}}
	if err := sc.ValidateCallbacks(noCallback, env); err != nil {
		t.Errorf("queue without callback should validate, got %v", err)
	}
}
