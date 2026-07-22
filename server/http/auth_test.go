// SPDX-License-Identifier: Apache-2.0

package http

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/server/auth"
	srvconfig "github.com/srjn45/rdq/server/config"
)

// authTokens is the token file used by the middleware tests: one principal per
// role on payments.*, a queue-scoped admin, and a platform-wide admin (grant *).
const authTokens = `{
  "principals": [
    {"name": "sub",   "token": "t_sub",   "grants": [{"queue": "payments.*", "role": "submitter"}]},
    {"name": "op",    "token": "t_op",    "grants": [{"queue": "payments.*", "role": "operator"}]},
    {"name": "qadmin","token": "t_qadmin","grants": [{"queue": "payments.*", "role": "admin"}]},
    {"name": "admin", "token": "t_admin", "grants": [{"queue": "*",          "role": "admin"}]}
  ]
}`

// newAuthServer builds a fully wired server (storage + config store + authz) and
// seeds two tasks so the GET /tasks/{id} queue-from-row path (API OI-1) is
// exercisable.
func newAuthServer(t *testing.T) *Server {
	t.Helper()
	ts, err := auth.ParseTokenStore([]byte(authTokens))
	if err != nil {
		t.Fatalf("token store: %v", err)
	}
	st := newFakeStorage()
	_ = st.Enqueue(t.Context(), envelope.Envelope{ID: "task_pay", Queue: "payments.charge"})
	_ = st.Enqueue(t.Context(), envelope.Envelope{ID: "task_ord", Queue: "orders"})
	return New(
		WithStorage(st),
		WithConfigStore(srvconfig.NewMemStore()),
		WithAuthorizer(auth.NewAuthorizer(ts)),
	)
}

// authReq routes a request carrying the given bearer token (empty = none).
func authReq(t *testing.T, s *Server, method, target, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(""))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

// TestAuthUnauthenticated: a /v1 request with no or an unknown token is 401
// UNAUTHENTICATED and carries WWW-Authenticate; health stays open.
func TestAuthUnauthenticated(t *testing.T) {
	s := newAuthServer(t)

	for _, tok := range []string{"", "t_unknown"} {
		rec := authReq(t, s, http.MethodGet, "/v1/queues/payments.charge/stats", tok)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("token %q: status = %d, want 401", tok, rec.Code)
		}
		if p := decodeProblem(t, rec); p.Code != CodeUnauthenticated {
			t.Errorf("token %q: code = %q, want UNAUTHENTICATED", tok, p.Code)
		}
		if rec.Header().Get("WWW-Authenticate") == "" {
			t.Errorf("token %q: missing WWW-Authenticate header", tok)
		}
	}

	// Health probes are outside the auth boundary (G11) — no token needed.
	if rec := authReq(t, s, http.MethodGet, "/healthz", ""); rec.Code != http.StatusOK {
		t.Errorf("healthz under auth server = %d, want 200 (open)", rec.Code)
	}
}

// TestAuthRoleMatrix drives the design 04 §5 matrix against the in-process
// handlers: each operation is allowed for roles at/above its level and FORBIDDEN
// below. "Allowed" is asserted as "not an auth rejection" (the handler then runs
// and returns its own status); denial is an exact 403 FORBIDDEN.
func TestAuthRoleMatrix(t *testing.T) {
	cases := []struct {
		name          string
		method, path  string
		allow, forbid []string // tokens that must pass / must be 403
	}{
		{
			"submit (submitter)", http.MethodPost, "/v1/queues/payments.charge/tasks",
			[]string{"t_sub", "t_op", "t_admin"}, nil,
		},
		{
			"stats (submitter)", http.MethodGet, "/v1/queues/payments.charge/stats",
			[]string{"t_sub", "t_op"}, nil,
		},
		{
			"dlq list (operator)", http.MethodGet, "/v1/queues/payments.charge/dlq",
			[]string{"t_op", "t_qadmin"}, []string{"t_sub"},
		},
		{
			"redrive (operator)", http.MethodPost, "/v1/queues/payments.charge/dlq:redrive",
			[]string{"t_op"}, []string{"t_sub"},
		},
		{
			"pause (operator)", http.MethodPost, "/v1/admin/queues/payments.charge:pause",
			[]string{"t_op"}, []string{"t_sub"},
		},
		{
			"get config (admin)", http.MethodGet, "/v1/admin/queues/payments.charge/config",
			[]string{"t_qadmin", "t_admin"}, []string{"t_sub", "t_op"},
		},
		{
			"delete queue (admin)", http.MethodDelete, "/v1/admin/queues/payments.charge",
			[]string{"t_qadmin", "t_admin"}, []string{"t_op"},
		},
		{
			// Cross-queue listing needs a global (*) admin grant; a payments.*
			// admin is not a platform admin (design 03 §5).
			"list queues (global admin)", http.MethodGet, "/v1/admin/queues",
			[]string{"t_admin"}, []string{"t_qadmin", "t_op"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newAuthServer(t)
			for _, tok := range tc.allow {
				rec := authReq(t, s, tc.method, tc.path, tok)
				if rec.Code == http.StatusUnauthorized || rec.Code == http.StatusForbidden {
					t.Errorf("token %q should be allowed, got %d", tok, rec.Code)
				}
			}
			for _, tok := range tc.forbid {
				rec := authReq(t, s, tc.method, tc.path, tok)
				if rec.Code != http.StatusForbidden {
					t.Errorf("token %q should be 403, got %d", tok, rec.Code)
				}
				if p := decodeProblem(t, rec); p.Code != CodeForbidden {
					t.Errorf("token %q: code = %q, want FORBIDDEN", tok, p.Code)
				}
			}
		})
	}
}

// TestAuthTaskLookupResolvesQueueFromRow: GET /tasks/{id} enforces the grant on
// the queue stored in the task row (API OI-1), not on the path.
func TestAuthTaskLookupResolvesQueueFromRow(t *testing.T) {
	s := newAuthServer(t)

	// payments submitter may read a task in payments.charge...
	if rec := authReq(t, s, http.MethodGet, "/v1/tasks/task_pay", "t_sub"); rec.Code == http.StatusForbidden {
		t.Errorf("payments submitter should read task_pay (payments.charge), got 403")
	}
	// ...but not a task in orders, which no grant covers.
	rec := authReq(t, s, http.MethodGet, "/v1/tasks/task_ord", "t_sub")
	if rec.Code != http.StatusForbidden {
		t.Errorf("payments submitter reading task_ord (orders) = %d, want 403", rec.Code)
	}
	// An unknown task id is a 404 (no existence oracle beyond the handler's own).
	if rec := authReq(t, s, http.MethodGet, "/v1/tasks/task_missing", "t_admin"); rec.Code != http.StatusNotFound {
		t.Errorf("missing task lookup = %d, want 404", rec.Code)
	}
}

// TestAuthDisabledByDefault: with no Authorizer configured, the /v1 boundary is
// a pass-through (dev/embedded mode) — no token required.
func TestAuthDisabledByDefault(t *testing.T) {
	s := New(WithStorage(newFakeStorage()))
	rec := authReq(t, s, http.MethodGet, "/v1/queues/q/stats", "")
	if rec.Code == http.StatusUnauthorized || rec.Code == http.StatusForbidden {
		t.Errorf("no authorizer configured should be open, got %d", rec.Code)
	}
}
