// SPDX-License-Identifier: Apache-2.0

// Package auth is the rdq-server authentication/authorization model (design 04
// §5). A caller presents a bearer token; a static token file maps that token to
// a principal holding per-(queue × role) grants. Roles are ordered —
// submitter ⊂ operator ⊂ admin — and queue grants support globs (payments.*).
//
// The package is deliberately transport-agnostic: it resolves a token to a
// principal and answers "may this principal act on this queue at this role?".
// The HTTP layer (server/http) owns request parsing, route→operation mapping,
// the GET /tasks/{id} queue-from-row resolution (API OI-1), and problem+json
// emission. Keeping the decision logic here makes the role matrix unit-testable
// without an HTTP server and leaves room for OIDC/other token sources later.
package auth

import (
	"fmt"
	"strings"

	"github.com/srjn45/rdq/core/policy"
)

// Role is an ordered privilege level (design 04 §5). Higher roles subsume the
// capabilities of lower ones: an operator may do anything a submitter may, and
// an admin anything an operator may. Compare with >= to test "at least".
type Role int

const (
	// RoleNone is the zero value: no privilege. It never satisfies a grant check.
	RoleNone Role = iota
	// RoleSubmitter may submit, get a task, and read stats.
	RoleSubmitter
	// RoleOperator adds DLQ list/redrive/purge and pause/resume.
	RoleOperator
	// RoleAdmin adds config read/write and queue delete.
	RoleAdmin
)

// String returns the wire name of the role (the token-file spelling).
func (r Role) String() string {
	switch r {
	case RoleSubmitter:
		return "submitter"
	case RoleOperator:
		return "operator"
	case RoleAdmin:
		return "admin"
	default:
		return "none"
	}
}

// ParseRole maps a token-file role string to a Role. Unknown roles are an error
// so a typo in the token file fails fast at load rather than silently granting
// or denying access.
func ParseRole(s string) (Role, error) {
	switch s {
	case "submitter":
		return RoleSubmitter, nil
	case "operator":
		return RoleOperator, nil
	case "admin":
		return RoleAdmin, nil
	default:
		return RoleNone, fmt.Errorf("unknown role %q (want submitter, operator, or admin)", s)
	}
}

// Grant is one authorization entry: a queue glob (design 04 §5 — e.g. payments.*
// or the catch-all *) paired with the role the principal holds on the matching
// queues.
type Grant struct {
	// Queue is a glob matched against the target queue name (core/policy.Glob).
	Queue string
	// Role is the privilege level held on queues matching Queue.
	Role Role
}

// Principal is an authenticated identity and its grants. It is immutable after
// load; the same value is shared across concurrent requests.
type Principal struct {
	// Name identifies the principal in audit records (design 04 §3, FR-18).
	Name string
	// Grants are OR-combined: the principal is allowed if ANY grant satisfies
	// the check.
	Grants []Grant
}

// Allows reports whether the principal may act on queue at (at least) need.
// A grant satisfies the check when its role is >= need AND its queue glob
// matches queue. Grants are OR-combined, so the most permissive matching grant
// wins.
func (p *Principal) Allows(queue string, need Role) bool {
	for _, g := range p.Grants {
		if g.Role >= need && policy.Glob(g.Queue, queue) {
			return true
		}
	}
	return false
}

// AllowsGlobal reports whether the principal may perform a cross-queue operation
// at (at least) need — e.g. GET /admin/queues, which enumerates every queue and
// so cannot be scoped to one. Only a catch-all grant (queue "*") whose role is
// >= need qualifies: a principal with admin on payments.* is not a platform-wide
// admin. This keeps the "server config is outside any queue owner's reach"
// boundary (design 03 §5) intact.
func (p *Principal) AllowsGlobal(need Role) bool {
	for _, g := range p.Grants {
		if g.Role >= need && g.Queue == "*" {
			return true
		}
	}
	return false
}

// Authorizer resolves a bearer credential to a Principal. v1 is backed by a
// static token file; the type is the seam a pluggable OIDC/JWKS source would
// slot into later (design 04 §5) without changing the HTTP middleware.
type Authorizer struct {
	store *TokenStore
}

// NewAuthorizer builds an Authorizer over a loaded token store.
func NewAuthorizer(store *TokenStore) *Authorizer {
	return &Authorizer{store: store}
}

// Authenticate parses an Authorization header value and resolves it to a
// principal. It returns ok=false when the header is missing, is not a Bearer
// credential, or the token is unknown — the caller maps that to 401
// UNAUTHENTICATED without distinguishing the cases (no token oracle).
func (a *Authorizer) Authenticate(authorization string) (*Principal, bool) {
	tok, ok := BearerToken(authorization)
	if !ok {
		return nil, false
	}
	return a.store.Lookup(tok)
}

// BearerToken extracts the token from an "Authorization: Bearer <token>" header
// value (RFC 6750). The scheme match is case-insensitive per RFC 7235; the token
// is returned verbatim. ok is false when the header is empty, not the Bearer
// scheme, or carries an empty token.
func BearerToken(authorization string) (token string, ok bool) {
	const prefix = "bearer "
	if len(authorization) <= len(prefix) || !strings.EqualFold(authorization[:len(prefix)], prefix) {
		return "", false
	}
	tok := strings.TrimSpace(authorization[len(prefix):])
	if tok == "" {
		return "", false
	}
	return tok, true
}
