// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
)

// TokenStore maps opaque bearer tokens to principals (design 04 §5). It is the
// v1 authentication source: a static JSON file, loaded once at startup and
// treated as immutable, so lookups are lock-free and safe for concurrent use.
type TokenStore struct {
	byToken map[string]*Principal
}

// tokenFile is the on-disk JSON schema. Unknown keys are rejected on load so a
// misspelled field fails fast rather than being silently ignored (design 03 §3
// — config typos must not survive to 3am).
//
// Example:
//
//	{
//	  "principals": [
//	    {
//	      "name": "payments-ops",
//	      "token": "tok_live_...",
//	      "grants": [
//	        {"queue": "payments.*", "role": "operator"},
//	        {"queue": "*",          "role": "submitter"}
//	      ]
//	    }
//	  ]
//	}
type tokenFile struct {
	Principals []principalEntry `json:"principals"`
}

type principalEntry struct {
	Name   string       `json:"name"`
	Token  string       `json:"token"`
	Grants []grantEntry `json:"grants"`
}

type grantEntry struct {
	Queue string `json:"queue"`
	Role  string `json:"role"`
}

// LoadTokenStore reads and parses a token file from disk.
func LoadTokenStore(path string) (*TokenStore, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("auth: read token file: %w", err)
	}
	return ParseTokenStore(data)
}

// ParseTokenStore parses and validates a token-file document. It enforces:
// non-empty principal names and tokens, unique tokens, at least one grant per
// principal, non-empty queue globs, and known role names — any violation is an
// error so an invalid token file never boots a server that would then
// mis-authorize (fail-closed at load).
func ParseTokenStore(data []byte) (*TokenStore, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var tf tokenFile
	if err := dec.Decode(&tf); err != nil {
		return nil, fmt.Errorf("auth: parse token file: %w", err)
	}

	byToken := make(map[string]*Principal, len(tf.Principals))
	for i, pe := range tf.Principals {
		if pe.Name == "" {
			return nil, fmt.Errorf("auth: principal[%d]: name is required", i)
		}
		if pe.Token == "" {
			return nil, fmt.Errorf("auth: principal %q: token is required", pe.Name)
		}
		if _, dup := byToken[pe.Token]; dup {
			return nil, fmt.Errorf("auth: principal %q: token is already assigned to another principal", pe.Name)
		}
		if len(pe.Grants) == 0 {
			return nil, fmt.Errorf("auth: principal %q: at least one grant is required", pe.Name)
		}
		grants := make([]Grant, len(pe.Grants))
		for j, ge := range pe.Grants {
			if ge.Queue == "" {
				return nil, fmt.Errorf("auth: principal %q grant[%d]: queue is required", pe.Name, j)
			}
			role, err := ParseRole(ge.Role)
			if err != nil {
				return nil, fmt.Errorf("auth: principal %q grant[%d]: %w", pe.Name, j, err)
			}
			grants[j] = Grant{Queue: ge.Queue, Role: role}
		}
		byToken[pe.Token] = &Principal{Name: pe.Name, Grants: grants}
	}
	return &TokenStore{byToken: byToken}, nil
}

// Lookup resolves a bearer token to its principal. ok is false for an unknown
// token. The comparison is a plain map hit; tokens are opaque high-entropy
// secrets, so there is no username enumeration surface to constant-time around.
func (ts *TokenStore) Lookup(token string) (*Principal, bool) {
	p, ok := ts.byToken[token]
	return p, ok
}
