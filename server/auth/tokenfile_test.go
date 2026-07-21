// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const goodTokenFile = `{
  "principals": [
    {
      "name": "payments-ops",
      "token": "tok_ops",
      "grants": [
        {"queue": "payments.*", "role": "operator"},
        {"queue": "*",          "role": "submitter"}
      ]
    },
    {
      "name": "platform-admin",
      "token": "tok_admin",
      "grants": [{"queue": "*", "role": "admin"}]
    }
  ]
}`

// TestParseTokenStoreAndLookup: a valid file loads and tokens resolve to the
// right principal and grants.
func TestParseTokenStoreAndLookup(t *testing.T) {
	ts, err := ParseTokenStore([]byte(goodTokenFile))
	if err != nil {
		t.Fatalf("ParseTokenStore: %v", err)
	}
	p, ok := ts.Lookup("tok_ops")
	if !ok || p.Name != "payments-ops" {
		t.Fatalf("Lookup(tok_ops) = %+v, %v", p, ok)
	}
	if !p.Allows("payments.charge", RoleOperator) {
		t.Error("payments-ops should have operator on payments.charge")
	}
	if !p.Allows("orders", RoleSubmitter) {
		t.Error("payments-ops should have submitter on any queue via *")
	}
	if _, ok := ts.Lookup("nope"); ok {
		t.Error("unknown token must not resolve")
	}
}

// TestParseTokenStoreRejectsBadInput: the loader fails fast on malformed files
// so an invalid token file never boots a mis-authorizing server.
func TestParseTokenStoreRejectsBadInput(t *testing.T) {
	cases := []struct {
		name string
		doc  string
		want string
	}{
		{"unknown field", `{"principals":[],"extra":1}`, "unknown field"},
		{"missing token", `{"principals":[{"name":"a","grants":[{"queue":"q","role":"admin"}]}]}`, "token is required"},
		{"missing name", `{"principals":[{"token":"t","grants":[{"queue":"q","role":"admin"}]}]}`, "name is required"},
		{"no grants", `{"principals":[{"name":"a","token":"t","grants":[]}]}`, "at least one grant"},
		{"empty queue", `{"principals":[{"name":"a","token":"t","grants":[{"queue":"","role":"admin"}]}]}`, "queue is required"},
		{"bad role", `{"principals":[{"name":"a","token":"t","grants":[{"queue":"q","role":"root"}]}]}`, "unknown role"},
		{"dup token", `{"principals":[
			{"name":"a","token":"t","grants":[{"queue":"q","role":"admin"}]},
			{"name":"b","token":"t","grants":[{"queue":"q","role":"admin"}]}]}`, "already assigned"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTokenStore([]byte(tc.doc))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ParseTokenStore err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

// TestLoadTokenStoreFromDisk: the file-path loader reads and parses.
func TestLoadTokenStoreFromDisk(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tokens.json")
	if err := os.WriteFile(path, []byte(goodTokenFile), 0o600); err != nil {
		t.Fatal(err)
	}
	ts, err := LoadTokenStore(path)
	if err != nil {
		t.Fatalf("LoadTokenStore: %v", err)
	}
	if _, ok := ts.Lookup("tok_admin"); !ok {
		t.Error("tok_admin should resolve after load")
	}
	if _, err := LoadTokenStore(filepath.Join(dir, "missing.json")); err == nil {
		t.Error("LoadTokenStore of a missing file should error")
	}
}
