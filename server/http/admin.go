// SPDX-License-Identifier: Apache-2.0

package http

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/srjn45/rdq/core/audit"
	coreconfig "github.com/srjn45/rdq/core/config"
	"github.com/srjn45/rdq/core/envelope"
	srvconfig "github.com/srjn45/rdq/server/config"
)

// queueSummary is one entry in the GET /admin/queues list response.
type queueSummary struct {
	Queue  string `json:"queue"`
	Paused bool   `json:"paused"`
}

// queueConfigResponse is the body for GET and PUT /admin/queues/{queue}/config.
type queueConfigResponse struct {
	Queue  string                  `json:"queue"`
	Config *coreconfig.QueueConfig `json:"config"`
	Paused bool                    `json:"paused"`
}

// mountAdmin registers the admin / config-plane routes (design 04 §3).
//
// Route layout under the already-stripped /v1 prefix:
//
//	GET    /admin/queues                   → list all queues + summary
//	GET    /admin/queues/{queue}/config    → get queue config
//	PUT    /admin/queues/{queue}/config    → upsert queue config (strict validation)
//	DELETE /admin/queues/{queue}           → delete queue (409 if non-empty)
//
// The existing ops.go handler owns /admin/queues/ (subtree) for the
// :pause/:resume action verbs; the patterns below are more specific and take
// precedence for their respective methods and paths (Go 1.22 ServeMux rules).
func (s *Server) mountAdmin(mux *http.ServeMux) {
	mux.HandleFunc("GET /admin/queues", s.handleListQueues)
	mux.HandleFunc("/admin/queues/{queue}/config", s.handleQueueConfig)
	mux.HandleFunc("DELETE /admin/queues/{queue}", s.handleDeleteQueue)
}

// handleListQueues serves GET /admin/queues → 200 [{queue, paused}].
func (s *Server) handleListQueues(w http.ResponseWriter, r *http.Request) {
	if s.configStore == nil {
		Error(w, r, CodeStorageUnavailable, WithDetail("config store not configured"))
		return
	}
	names, err := s.configStore.List()
	if err != nil {
		Error(w, r, CodeInternal, WithDetail("config store: "+err.Error()))
		return
	}
	out := make([]queueSummary, len(names))
	for i, name := range names {
		out[i] = queueSummary{Queue: name, Paused: s.configStore.IsPaused(name)}
	}
	writeJSON(w, http.StatusOK, out)
}

// handleQueueConfig dispatches GET and PUT for /admin/queues/{queue}/config,
// returning 405 for any other method so callers get a proper Allow header.
func (s *Server) handleQueueConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handleGetQueueConfig(w, r)
	case http.MethodPut:
		s.handlePutQueueConfig(w, r)
	default:
		w.Header().Set("Allow", "GET, PUT")
		Error(w, r, CodeMethodNotAllowed, WithDetail("only GET and PUT are allowed on this path"))
	}
}

// handleGetQueueConfig serves GET /admin/queues/{queue}/config → 200 config.
func (s *Server) handleGetQueueConfig(w http.ResponseWriter, r *http.Request) {
	if s.configStore == nil {
		Error(w, r, CodeStorageUnavailable, WithDetail("config store not configured"))
		return
	}
	queue := r.PathValue("queue")
	if err := envelope.ValidateQueue(queue); err != nil {
		Error(w, r, CodeQueueNotFound, WithDetail("invalid queue name: "+err.Error()))
		return
	}

	entry, err := s.configStore.Get(queue)
	if err != nil {
		if errors.Is(err, srvconfig.ErrNotFound) {
			Error(w, r, CodeQueueNotFound, WithDetail("queue "+queue+" is not configured"))
			return
		}
		Error(w, r, CodeInternal, WithDetail("config store: "+err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, queueConfigResponse{
		Queue:  queue,
		Config: entry.Config,
		Paused: entry.Paused,
	})
}

// handlePutQueueConfig serves PUT /admin/queues/{queue}/config → 200 validated
// config. The body is a QueueConfig in JSON; unknown keys are rejected (strict
// validation, design 03 §3). The change takes effect at next claim (design 03 §1).
func (s *Server) handlePutQueueConfig(w http.ResponseWriter, r *http.Request) {
	if s.configStore == nil {
		Error(w, r, CodeStorageUnavailable, WithDetail("config store not configured"))
		return
	}
	queue := r.PathValue("queue")
	if err := envelope.ValidateQueue(queue); err != nil {
		Error(w, r, CodeQueueNotFound, WithDetail("invalid queue name: "+err.Error()))
		return
	}

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var qc coreconfig.QueueConfig
	if err := dec.Decode(&qc); err != nil {
		Error(w, r, CodeInvalidTask, WithDetail("invalid config JSON: "+err.Error()))
		return
	}

	if err := coreconfig.ValidateQueue(&qc, queue); err != nil {
		Error(w, r, CodeInvalidTask, WithDetail(err.Error()))
		return
	}

	if err := s.configStore.Put(queue, &qc); err != nil {
		_ = s.audit().Emit(audit.Record{
			Timestamp: time.Now().UTC(), Principal: principalName(r.Context()),
			Action: audit.ActionConfigWrite, Queue: queue, Count: -1,
			Outcome: audit.OutcomeFailure, ErrorMessage: err.Error(),
		})
		Error(w, r, CodeInternal, WithDetail("config store: "+err.Error()))
		return
	}
	_ = s.audit().Emit(audit.Record{
		Timestamp: time.Now().UTC(), Principal: principalName(r.Context()),
		Action: audit.ActionConfigWrite, Queue: queue, Count: -1,
		Outcome: audit.OutcomeSuccess,
	})

	paused := s.configStore.IsPaused(queue)
	writeJSON(w, http.StatusOK, queueConfigResponse{Queue: queue, Config: &qc, Paused: paused})
}

// handleDeleteQueue serves DELETE /admin/queues/{queue} → 204 on success, or
// 409 CONFLICT when the queue still has pending/in-flight/DLQ tasks (design 04 §3).
func (s *Server) handleDeleteQueue(w http.ResponseWriter, r *http.Request) {
	if s.configStore == nil {
		Error(w, r, CodeStorageUnavailable, WithDetail("config store not configured"))
		return
	}
	queue := r.PathValue("queue")
	if err := envelope.ValidateQueue(queue); err != nil {
		Error(w, r, CodeQueueNotFound, WithDetail("invalid queue name: "+err.Error()))
		return
	}

	// Guard: refuse deletion when any tasks remain (design 04 §3).
	if s.storage != nil {
		stats, err := s.storage.Stats(r.Context(), queue)
		if err != nil {
			Error(w, r, CodeInternal, WithDetail("storage: "+err.Error()))
			return
		}
		if stats.Pending > 0 || stats.InFlight > 0 || stats.DLQDepth > 0 {
			Error(w, r, CodeConflict,
				WithDetail("queue "+queue+" is not empty (pending/in-flight/dlq tasks exist)"))
			return
		}
	}

	if err := s.configStore.Delete(queue); err != nil {
		if errors.Is(err, srvconfig.ErrNotFound) {
			Error(w, r, CodeQueueNotFound, WithDetail("queue "+queue+" is not configured"))
			return
		}
		Error(w, r, CodeInternal, WithDetail("config store: "+err.Error()))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
