// SPDX-License-Identifier: Apache-2.0

package http

import (
	"net/http"
	"strings"
)

// mountOps registers the queue pause/resume routes under /admin/queues/.
//
// The API shape is /admin/queues/{queue}:pause — the action suffix is in the
// same path segment as the queue name, which Go 1.22's ServeMux does not
// support as a wildcard pattern. A prefix handler on /admin/queues/ and manual
// suffix parsing keeps the URL shape spec-faithful without a third-party router.
func (s *Server) mountOps(mux *http.ServeMux) {
	mux.HandleFunc("/admin/queues/", s.handleAdminQueueAction)
}

// handleAdminQueueAction dispatches:
//
//	POST /admin/queues/{queue}:pause  → 204 (stop claiming; submits still accepted)
//	POST /admin/queues/{queue}:resume → 204
//
// Pause state is written to both the in-process sync.Map (fast path for claim
// loops) and the ConfigStore (T5.4) so it survives server restarts.
func (s *Server) handleAdminQueueAction(w http.ResponseWriter, r *http.Request) {
	const prefix = "/admin/queues/"
	rest := strings.TrimPrefix(r.URL.Path, prefix)

	var queue, action string
	if q, ok := strings.CutSuffix(rest, ":pause"); ok {
		queue, action = q, "pause"
	} else if q, ok := strings.CutSuffix(rest, ":resume"); ok {
		queue, action = q, "resume"
	} else {
		Error(w, r, CodeNotFound, WithDetail("no route matches "+r.URL.Path))
		return
	}
	if queue == "" || strings.ContainsRune(queue, '/') {
		Error(w, r, CodeNotFound, WithDetail("invalid queue name in path"))
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		Error(w, r, CodeMethodNotAllowed, WithDetail("only POST is allowed on this path"))
		return
	}

	switch action {
	case "pause":
		s.paused.Store(queue, struct{}{})
		if s.configStore != nil {
			_ = s.configStore.SetPaused(queue, true)
		}
		w.WriteHeader(http.StatusNoContent)
	case "resume":
		s.paused.Delete(queue)
		if s.configStore != nil {
			_ = s.configStore.SetPaused(queue, false)
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// IsPaused reports whether claiming is suspended for queue. When a ConfigStore
// is present (T5.4), it is authoritative for pause state (survives restart);
// otherwise the in-process sync.Map is used (in-memory only).
func (s *Server) IsPaused(queue string) bool {
	if s.configStore != nil {
		return s.configStore.IsPaused(queue)
	}
	_, ok := s.paused.Load(queue)
	return ok
}
