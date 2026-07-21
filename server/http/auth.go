// SPDX-License-Identifier: Apache-2.0

package http

import (
	"errors"
	"net/http"
	"strings"

	"github.com/srjn45/rdq/core/spi"
	"github.com/srjn45/rdq/server/auth"
)

// authMiddleware enforces the /v1 auth boundary (design 04 §5) when an
// Authorizer is configured. It runs after StripPrefix, so r.URL.Path is already
// the /v1-relative path (e.g. /queues/q/tasks) that mirrors the mount patterns.
//
// Flow: authenticate the bearer token (401 on miss), classify the request into
// an operation (target queue + minimum role), then enforce the principal's
// grant (403 on deny). Requests that classify to no known operation — unknown
// routes or wrong methods — pass through so the route mux emits the correct
// 404/405 rather than a misleading auth error.
func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := s.authz.Authenticate(r.Header.Get("Authorization"))
		if !ok {
			w.Header().Set("WWW-Authenticate", "Bearer")
			Error(w, r, CodeUnauthenticated, WithDetail("missing or invalid bearer token"))
			return
		}

		op, matched := classifyAuthOp(r.Method, r.URL.Path)
		if !matched {
			next.ServeHTTP(w, r) // unknown route/method → mux emits 404/405
			return
		}

		allowed, prob := s.authorize(r, principal, op)
		if prob != nil {
			WriteProblem(w, prob)
			return
		}
		if !allowed {
			Error(w, r, CodeForbidden,
				WithDetail("principal "+principal.Name+" lacks "+op.role.String()+" on this queue"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// authOp is the authorization requirement classified from a request: the
// minimum role and how the target queue is determined.
type authOp struct {
	role   auth.Role
	queue  string // target queue when known from the path
	global bool   // cross-queue op (GET /admin/queues) — needs a "*" grant
	// fromTask is set for GET /tasks/{id}: the queue is resolved from the stored
	// task row, not the path (API OI-1). taskID carries the id to look up.
	fromTask bool
	taskID   string
}

// authorize resolves op's target queue (looking up the task row for the
// fromTask case) and checks principal's grant. It returns (allowed, nil) on a
// clean decision, or (false, problem) when the queue could not be resolved
// (storage unavailable, or task not found → the same 404 the handler would give).
func (s *Server) authorize(r *http.Request, principal *auth.Principal, op authOp) (bool, *Problem) {
	switch {
	case op.global:
		return principal.AllowsGlobal(op.role), nil
	case op.fromTask:
		if s.storage == nil {
			return false, NewProblem(CodeStorageUnavailable, r.URL.Path, WithDetail("storage not configured"))
		}
		task, err := s.storage.Get(r.Context(), op.taskID)
		if err != nil {
			if errors.Is(err, spi.ErrNotFound) {
				// Do not reveal a task the caller could not read anyway: a missing
				// task is a 404, identical to the handler's own response.
				return false, NewProblem(CodeNotFound, r.URL.Path, WithDetail("no task with id "+op.taskID))
			}
			return false, NewProblem(CodeInternal, r.URL.Path, WithDetail("storage: "+err.Error()))
		}
		return principal.Allows(task.Queue, op.role), nil
	default:
		return principal.Allows(op.queue, op.role), nil
	}
}

// classifyAuthOp maps a (method, /v1-relative path) onto its authorization
// requirement, mirroring the route mounts in tasks.go/dlq.go/ops.go/admin.go.
// matched is false for any path+method that is not a known API operation; the
// caller forwards those unchanged so the mux produces the right 404/405.
//
// Role matrix (design 04 §5): submit/get/stats need submitter; DLQ ops and
// pause/resume need operator; config read/write and queue delete need admin.
func classifyAuthOp(method, path string) (authOp, bool) {
	segs := strings.Split(strings.Trim(path, "/"), "/")

	switch segs[0] {
	case "tasks":
		// GET /tasks/{id} — queue resolved from the row (API OI-1).
		if len(segs) == 2 && method == http.MethodGet && segs[1] != "" {
			return authOp{role: auth.RoleSubmitter, fromTask: true, taskID: segs[1]}, true
		}

	case "queues":
		// /queues/{queue}/{tail}
		if len(segs) == 3 && segs[1] != "" {
			queue, tail := segs[1], segs[2]
			switch {
			case tail == "tasks" && method == http.MethodPost,
				tail == "tasks:batch" && method == http.MethodPost,
				tail == "stats" && method == http.MethodGet:
				return authOp{role: auth.RoleSubmitter, queue: queue}, true
			case tail == "dlq" && method == http.MethodGet,
				tail == "dlq:redrive" && method == http.MethodPost,
				tail == "dlq:purge" && method == http.MethodPost:
				return authOp{role: auth.RoleOperator, queue: queue}, true
			}
		}

	case "admin":
		// GET /admin/queues — cross-queue listing (needs a global admin grant).
		if len(segs) == 2 && segs[1] == "queues" && method == http.MethodGet {
			return authOp{role: auth.RoleAdmin, global: true}, true
		}
		if len(segs) >= 3 && segs[1] == "queues" {
			// POST /admin/queues/{queue}:pause | :resume — operator.
			if len(segs) == 3 && method == http.MethodPost {
				if q, ok := strings.CutSuffix(segs[2], ":pause"); ok && q != "" {
					return authOp{role: auth.RoleOperator, queue: q}, true
				}
				if q, ok := strings.CutSuffix(segs[2], ":resume"); ok && q != "" {
					return authOp{role: auth.RoleOperator, queue: q}, true
				}
			}
			// DELETE /admin/queues/{queue} — admin.
			if len(segs) == 3 && method == http.MethodDelete && !strings.ContainsRune(segs[2], ':') {
				return authOp{role: auth.RoleAdmin, queue: segs[2]}, true
			}
			// GET|PUT /admin/queues/{queue}/config — admin.
			if len(segs) == 4 && segs[3] == "config" && segs[2] != "" &&
				(method == http.MethodGet || method == http.MethodPut) {
				return authOp{role: auth.RoleAdmin, queue: segs[2]}, true
			}
		}
	}

	return authOp{}, false
}
