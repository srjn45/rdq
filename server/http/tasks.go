// SPDX-License-Identifier: Apache-2.0

package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/spi"
	sdksubmit "github.com/srjn45/rdq/sdk-go/submit"
)

// defaultMaxPayloadBytes is the server-wide default ceiling for a decoded task
// payload. Per-queue limits (design 04 §1) layer on top once ConfigStore (T5.4)
// lands; until then this single bound applies.
const defaultMaxPayloadBytes int64 = 1 << 20 // 1 MiB

// submitTaskRequest is the JSON body for POST /queues/{queue}/tasks and each
// element of the array body for POST /queues/{queue}/tasks:batch.
type submitTaskRequest struct {
	ID                 string            `json:"id,omitempty"`
	HandlerRef         string            `json:"handler_ref"`
	Payload            []byte            `json:"payload"`
	PayloadContentType string            `json:"payload_content_type"`
	Headers            map[string]string `json:"headers,omitempty"`
}

// batchItemResult is one entry in the 207 Multi-Status response for
// POST /queues/{queue}/tasks:batch (design 04 §1, OI-2).
type batchItemResult struct {
	Index    int                `json:"index"`
	Status   int                `json:"status"`
	Envelope *envelope.Envelope `json:"envelope,omitempty"`
	Error    *Problem           `json:"error,omitempty"`
}

// mountTasks registers the T5.2 data-plane routes on mux. T5.3 (dlq.go) and
// T5.4 (admin.go) call their own mount helpers alongside this one from v1Handler.
func (s *Server) mountTasks(mux *http.ServeMux) {
	mux.HandleFunc("/queues/{queue}/tasks", only(http.MethodPost, s.handleSubmitTask))
	mux.HandleFunc("/queues/{queue}/tasks:batch", only(http.MethodPost, s.handleBatchSubmit))
	mux.HandleFunc("/tasks/{id}", only(http.MethodGet, s.handleGetTask))
}

// handleSubmitTask serves POST /queues/{queue}/tasks → 202 Accepted with the
// full envelope (idempotent by task id, design 04 §1).
func (s *Server) handleSubmitTask(w http.ResponseWriter, r *http.Request) {
	if s.storage == nil {
		Error(w, r, CodeStorageUnavailable, WithDetail("storage not configured"))
		return
	}
	queue := r.PathValue("queue")

	// Bound the body reader before decoding to prevent flooding.
	r.Body = http.MaxBytesReader(w, r.Body, s.maxPayloadBytes*2+4096)
	var req submitTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			Error(w, r, CodePayloadTooLarge, WithDetail("request body exceeds limit"))
			return
		}
		Error(w, r, CodeInvalidTask, WithDetail("invalid JSON: "+err.Error()))
		return
	}

	env, prob := s.buildEnvelope(r.URL.Path, queue, &req)
	if prob != nil {
		WriteProblem(w, prob)
		return
	}

	stored, prob := s.enqueueAndGet(r.Context(), r.URL.Path, env)
	if prob != nil {
		WriteProblem(w, prob)
		return
	}
	writeJSON(w, http.StatusAccepted, stored)
}

// handleBatchSubmit serves POST /queues/{queue}/tasks:batch → 207 Multi-Status
// with per-item results (design 04 §1, OI-2). A failure on one item does not
// prevent the others from being enqueued.
func (s *Server) handleBatchSubmit(w http.ResponseWriter, r *http.Request) {
	if s.storage == nil {
		Error(w, r, CodeStorageUnavailable, WithDetail("storage not configured"))
		return
	}
	queue := r.PathValue("queue")

	const batchBodyMax = 16 << 20 // 16 MiB: generous ceiling for many items
	r.Body = http.MaxBytesReader(w, r.Body, batchBodyMax)

	var items []submitTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&items); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			Error(w, r, CodePayloadTooLarge, WithDetail("request body exceeds limit"))
			return
		}
		Error(w, r, CodeInvalidTask, WithDetail("invalid JSON: "+err.Error()))
		return
	}

	results := make([]batchItemResult, len(items))
	for i := range items {
		env, prob := s.buildEnvelope(r.URL.Path, queue, &items[i])
		if prob != nil {
			results[i] = batchItemResult{Index: i, Status: prob.Status, Error: prob}
			continue
		}
		stored, prob := s.enqueueAndGet(r.Context(), r.URL.Path, env)
		if prob != nil {
			results[i] = batchItemResult{Index: i, Status: prob.Status, Error: prob}
			continue
		}
		results[i] = batchItemResult{Index: i, Status: http.StatusAccepted, Envelope: stored}
	}
	writeJSON(w, http.StatusMultiStatus, results)
}

// handleGetTask serves GET /tasks/{id} → 200 with the full envelope in any
// status (PENDING/IN_FLIGHT/SUCCEEDED/DEAD), 404 NOT_FOUND if absent
// (design 04 §1, G4).
func (s *Server) handleGetTask(w http.ResponseWriter, r *http.Request) {
	if s.storage == nil {
		Error(w, r, CodeStorageUnavailable, WithDetail("storage not configured"))
		return
	}
	id := r.PathValue("id")

	task, err := s.storage.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, spi.ErrNotFound) {
			Error(w, r, CodeNotFound, WithDetail("no task with id "+id))
			return
		}
		Error(w, r, CodeInternal, WithDetail("storage: "+err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, task)
}

// buildEnvelope validates the submit request and builds an Envelope via the
// sdk-go/submit path so this handler shares the same validation and ID
// generation as the Go SDK client. Returns (nil, problem) on failure.
func (s *Server) buildEnvelope(instance, queue string, req *submitTaskRequest) (*envelope.Envelope, *Problem) {
	if req.PayloadContentType == "" {
		return nil, NewProblem(CodeInvalidTask, instance, WithDetail("payload_content_type is required"))
	}
	if int64(len(req.Payload)) > s.maxPayloadBytes {
		return nil, NewProblem(CodePayloadTooLarge, instance)
	}

	var opts []sdksubmit.Option
	if req.ID != "" {
		opts = append(opts, sdksubmit.WithID(req.ID))
	}
	opts = append(opts, sdksubmit.WithContentType(req.PayloadContentType))
	if len(req.Headers) > 0 {
		opts = append(opts, sdksubmit.WithHeaders(req.Headers))
	}

	env, err := sdksubmit.Submit(queue, req.HandlerRef, req.Payload, opts...)
	if err != nil {
		return nil, NewProblem(CodeInvalidTask, instance, WithDetail(err.Error()))
	}
	return env, nil
}

// enqueueAndGet calls Enqueue then Get so that a re-submit of an existing id
// returns the CURRENT stored envelope rather than the caller-built one.
func (s *Server) enqueueAndGet(ctx context.Context, instance string, env *envelope.Envelope) (*envelope.Envelope, *Problem) {
	if err := s.storage.Enqueue(ctx, *env); err != nil {
		if errors.Is(err, spi.ErrIDConflict) {
			return nil, NewProblem(CodeIDConflict, instance, WithDetail("task id already exists in a different queue"))
		}
		return nil, NewProblem(CodeInternal, instance, WithDetail("enqueue: "+err.Error()))
	}
	stored, err := s.storage.Get(ctx, env.ID)
	if err != nil {
		return nil, NewProblem(CodeInternal, instance, WithDetail("get after enqueue: "+err.Error()))
	}
	return &stored, nil
}
