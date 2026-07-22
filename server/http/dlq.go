// SPDX-License-Identifier: Apache-2.0

package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/srjn45/rdq/core/audit"
	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/spi"
)

// dlqListResponse is the JSON body for GET /queues/{queue}/dlq.
type dlqListResponse struct {
	Tasks      []envelope.Envelope `json:"tasks"`
	NextCursor spi.Cursor          `json:"next_cursor,omitempty"`
}

// selectorRequest is the body for dlq:redrive and dlq:purge. Supply ids XOR
// filter; an empty body selects nothing and returns count:0.
type selectorRequest struct {
	IDs    []string       `json:"ids,omitempty"`
	Filter *spi.DLQFilter `json:"filter,omitempty"`
}

// countResponse carries the authoritative count of tasks affected by a
// redrive or purge operation (design 04 §2, SPI OI-2).
type countResponse struct {
	Count int `json:"count"`
}

// statsResponse is the 200 body for GET /queues/{queue}/stats.
// OldestPendingAgeMs is milliseconds so clients parse it without Go-specific logic.
type statsResponse struct {
	Pending            int64 `json:"pending"`
	InFlight           int64 `json:"in_flight"`
	DLQDepth           int64 `json:"dlq_depth"`
	OldestPendingAgeMs int64 `json:"oldest_pending_age_ms"`
}

// mountDLQ registers the DLQ browse/mutate routes and the per-queue stats route.
func (s *Server) mountDLQ(mux *http.ServeMux) {
	mux.HandleFunc("/queues/{queue}/dlq", only(http.MethodGet, s.handleDLQList))
	mux.HandleFunc("/queues/{queue}/dlq:redrive", only(http.MethodPost, s.handleRedrive))
	mux.HandleFunc("/queues/{queue}/dlq:purge", only(http.MethodPost, s.handlePurge))
	mux.HandleFunc("/queues/{queue}/stats", only(http.MethodGet, s.handleStats))
}

// handleDLQList serves GET /queues/{queue}/dlq with cursor-based pagination
// and optional filtering via query parameters.
func (s *Server) handleDLQList(w http.ResponseWriter, r *http.Request) {
	if s.storage == nil {
		Error(w, r, CodeStorageUnavailable, WithDetail("storage not configured"))
		return
	}
	queue := r.PathValue("queue")
	q := r.URL.Query()

	f := spi.DLQFilter{
		ErrorType:  q.Get("error_type"),
		HandlerRef: q.Get("handler_ref"),
	}
	if v := q.Get("from"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			Error(w, r, CodeInvalidTask, WithDetail("from: must be RFC3339 time"))
			return
		}
		f.DeadLetteredAfter = &t
	}
	if v := q.Get("to"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			Error(w, r, CodeInvalidTask, WithDetail("to: must be RFC3339 time"))
			return
		}
		f.DeadLetteredBefore = &t
	}
	page := spi.Page{}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			Error(w, r, CodeInvalidTask, WithDetail("limit: must be a positive integer"))
			return
		}
		page.Limit = n
	}
	if v := q.Get("cursor"); v != "" {
		page.After = spi.Cursor(v)
	}

	tasks, next, err := s.storage.DLQList(r.Context(), queue, f, page)
	if err != nil {
		if errors.Is(err, spi.ErrStaleCursor) {
			Error(w, r, CodeStaleCursor, WithDetail("pagination cursor is no longer valid"))
			return
		}
		Error(w, r, CodeInternal, WithDetail("storage: "+err.Error()))
		return
	}
	if tasks == nil {
		tasks = []envelope.Envelope{}
	}
	writeJSON(w, http.StatusOK, dlqListResponse{Tasks: tasks, NextCursor: next})
}

// handleRedrive serves POST /queues/{queue}/dlq:redrive.
func (s *Server) handleRedrive(w http.ResponseWriter, r *http.Request) {
	s.handleDLQMutate(w, r, audit.ActionRedrive, func(ctx context.Context, queue string, sel spi.Selector) (int, error) {
		return s.storage.Redrive(ctx, queue, sel)
	})
}

// handlePurge serves POST /queues/{queue}/dlq:purge.
func (s *Server) handlePurge(w http.ResponseWriter, r *http.Request) {
	s.handleDLQMutate(w, r, audit.ActionPurge, func(ctx context.Context, queue string, sel spi.Selector) (int, error) {
		return s.storage.Purge(ctx, queue, sel)
	})
}

// handleDLQMutate is the shared body for redrive and purge: decode the
// selector, apply filter-streaming when the backend lacks FilterPushdown
// (design 04 §2, SPI OI-2), call op, return { count }. It emits an audit
// record through s.audit() on both success and failure.
func (s *Server) handleDLQMutate(w http.ResponseWriter, r *http.Request, action audit.Action, op func(context.Context, string, spi.Selector) (int, error)) {
	if s.storage == nil {
		Error(w, r, CodeStorageUnavailable, WithDetail("storage not configured"))
		return
	}
	queue := r.PathValue("queue")

	var req selectorRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		Error(w, r, CodeInvalidTask, WithDetail("invalid JSON: "+err.Error()))
		return
	}
	if len(req.IDs) > 0 && req.Filter != nil {
		Error(w, r, CodeInvalidTask, WithDetail("supply ids or filter, not both"))
		return
	}

	sel := spi.Selector{IDs: req.IDs, Filter: req.Filter}
	selDesc := selectorDescription(req)
	principal := principalName(r.Context())

	// Filter-streaming: back-fill IDs from DLQList pages when the backend
	// lacks native FilterPushdown. Entries dead-lettered mid-stream are not
	// included; the returned count is authoritative (design 04 §2, SPI OI-2).
	if sel.Filter != nil && !s.storage.Capabilities().FilterPushdown {
		ids, err := s.collectByFilter(r.Context(), queue, *sel.Filter)
		if err != nil {
			_ = s.audit().Emit(audit.Record{
				Timestamp: time.Now().UTC(), Principal: principal,
				Action: action, Queue: queue, Selector: selDesc,
				Count: -1, Outcome: audit.OutcomeFailure, ErrorMessage: err.Error(),
			})
			Error(w, r, CodeInternal, WithDetail("dlq scan: "+err.Error()))
			return
		}
		sel = spi.Selector{IDs: ids}
	}

	n, err := op(r.Context(), queue, sel)
	if err != nil {
		_ = s.audit().Emit(audit.Record{
			Timestamp: time.Now().UTC(), Principal: principal,
			Action: action, Queue: queue, Selector: selDesc,
			Count: -1, Outcome: audit.OutcomeFailure, ErrorMessage: err.Error(),
		})
		Error(w, r, CodeInternal, WithDetail("storage: "+err.Error()))
		return
	}
	_ = s.audit().Emit(audit.Record{
		Timestamp: time.Now().UTC(), Principal: principal,
		Action: action, Queue: queue, Selector: selDesc,
		Count: n, Outcome: audit.OutcomeSuccess,
	})
	writeJSON(w, http.StatusOK, countResponse{Count: n})
}

// selectorDescription returns a short human-readable description of a
// selectorRequest for audit records: "ids:[x,y]", "ids:N", "filter:{...}",
// or "all".
func selectorDescription(req selectorRequest) string {
	if len(req.IDs) > 0 {
		if len(req.IDs) <= 3 {
			return "ids:[" + strings.Join(req.IDs, ",") + "]"
		}
		return fmt.Sprintf("ids:%d", len(req.IDs))
	}
	if req.Filter != nil {
		var parts []string
		if req.Filter.ErrorType != "" {
			parts = append(parts, "error_type:"+req.Filter.ErrorType)
		}
		if req.Filter.HandlerRef != "" {
			parts = append(parts, "handler_ref:"+req.Filter.HandlerRef)
		}
		if len(parts) > 0 {
			return "filter:{" + strings.Join(parts, ",") + "}"
		}
		return "filter:{}"
	}
	return "all"
}

// collectByFilter pages DLQList until exhausted, collecting all matching IDs.
// Used by the filter-streaming path when the backend lacks FilterPushdown.
func (s *Server) collectByFilter(ctx context.Context, queue string, f spi.DLQFilter) ([]string, error) {
	var ids []string
	var cursor spi.Cursor
	for {
		tasks, next, err := s.storage.DLQList(ctx, queue, f, spi.Page{Limit: 500, After: cursor})
		if err != nil {
			return nil, err
		}
		for i := range tasks {
			ids = append(ids, tasks[i].ID)
		}
		if next == "" {
			break
		}
		cursor = next
	}
	return ids, nil
}

// handleStats serves GET /queues/{queue}/stats returning a per-queue snapshot.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if s.storage == nil {
		Error(w, r, CodeStorageUnavailable, WithDetail("storage not configured"))
		return
	}
	queue := r.PathValue("queue")

	st, err := s.storage.Stats(r.Context(), queue)
	if err != nil {
		Error(w, r, CodeInternal, WithDetail("storage: "+err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, statsResponse{
		Pending:            st.Pending,
		InFlight:           st.InFlight,
		DLQDepth:           st.DLQDepth,
		OldestPendingAgeMs: st.OldestPendingAge.Milliseconds(),
	})
}
