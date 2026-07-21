// SPDX-License-Identifier: Apache-2.0

package http

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/spi"
)

// --- extended fake storage with real DLQ behaviour ---

// dlqStorage embeds fakeStorage and adds functional DLQ list/redrive/purge/stats.
type dlqStorage struct {
	*fakeStorage
	dlqTasks       []envelope.Envelope
	filterPushdown bool
	// capture last selector IDs passed to Redrive/Purge for streaming-path assertions
	lastRedriveIDs []string
	lastPurgeIDs   []string
	statsResult    spi.Stats
}

func newDLQStorage() *dlqStorage {
	return &dlqStorage{fakeStorage: newFakeStorage()}
}

func (d *dlqStorage) DLQList(_ context.Context, _ string, _ spi.DLQFilter, p spi.Page) ([]envelope.Envelope, spi.Cursor, error) {
	tasks := d.dlqTasks
	if p.After != "" {
		// cursor encodes the ID of the last returned task (simple test impl)
		start := 0
		for i, t := range tasks {
			if spi.Cursor(t.ID) == p.After {
				start = i + 1
				break
			}
		}
		tasks = tasks[start:]
	}
	limit := p.Limit
	if limit <= 0 || limit > len(tasks) {
		limit = len(tasks)
	}
	page := tasks[:limit]
	var next spi.Cursor
	if limit < len(tasks) {
		next = spi.Cursor(page[len(page)-1].ID)
	}
	out := make([]envelope.Envelope, len(page))
	copy(out, page)
	return out, next, nil
}

func (d *dlqStorage) Redrive(_ context.Context, _ string, sel spi.Selector) (int, error) {
	if len(sel.IDs) > 0 {
		d.lastRedriveIDs = sel.IDs
		return len(sel.IDs), nil
	}
	if sel.Filter != nil {
		return len(d.dlqTasks), nil
	}
	return 0, nil
}

func (d *dlqStorage) Purge(_ context.Context, _ string, sel spi.Selector) (int, error) {
	if len(sel.IDs) > 0 {
		d.lastPurgeIDs = sel.IDs
		return len(sel.IDs), nil
	}
	if sel.Filter != nil {
		return len(d.dlqTasks), nil
	}
	return 0, nil
}

func (d *dlqStorage) Stats(_ context.Context, _ string) (spi.Stats, error) {
	return d.statsResult, nil
}

func (d *dlqStorage) Capabilities() spi.Capabilities {
	return spi.Capabilities{FilterPushdown: d.filterPushdown}
}

// staleCursorStorage returns ErrStaleCursor from DLQList when a cursor is set.
type staleCursorStorage struct {
	*dlqStorage
}

func (s *staleCursorStorage) DLQList(_ context.Context, _ string, _ spi.DLQFilter, p spi.Page) ([]envelope.Envelope, spi.Cursor, error) {
	if p.After != "" {
		return nil, "", spi.ErrStaleCursor
	}
	return []envelope.Envelope{}, "", nil
}

func deadEnvelope(id, queue string) envelope.Envelope {
	return envelope.Envelope{ID: id, Queue: queue}
}

// --- DLQ list ---

func TestDLQList_200_Empty(t *testing.T) {
	s := newTestServer(t, newDLQStorage())
	rec := do(t, s, http.MethodGet, "/v1/queues/q/dlq")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", rec.Code, rec.Body)
	}
	var resp dlqListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Tasks) != 0 {
		t.Errorf("tasks = %d, want 0", len(resp.Tasks))
	}
}

func TestDLQList_200_WithTasks(t *testing.T) {
	st := newDLQStorage()
	st.dlqTasks = []envelope.Envelope{
		deadEnvelope("id1", "q"),
		deadEnvelope("id2", "q"),
	}
	s := newTestServer(t, st)
	rec := do(t, s, http.MethodGet, "/v1/queues/q/dlq")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", rec.Code, rec.Body)
	}
	var resp dlqListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Tasks) != 2 {
		t.Errorf("tasks = %d, want 2", len(resp.Tasks))
	}
}

func TestDLQList_503_NoStorage(t *testing.T) {
	s := New()
	rec := do(t, s, http.MethodGet, "/v1/queues/q/dlq")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func TestDLQList_409_StaleCursor(t *testing.T) {
	s := newTestServer(t, &staleCursorStorage{dlqStorage: newDLQStorage()})
	rec := do(t, s, http.MethodGet, "/v1/queues/q/dlq?cursor=stale-token")
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409\nbody: %s", rec.Code, rec.Body)
	}
	if p := decodeProblem(t, rec); p.Code != CodeStaleCursor {
		t.Errorf("code = %q, want STALE_CURSOR", p.Code)
	}
}

func TestDLQList_422_BadLimit(t *testing.T) {
	s := newTestServer(t, newDLQStorage())
	rec := do(t, s, http.MethodGet, "/v1/queues/q/dlq?limit=bad")
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", rec.Code)
	}
}

// --- redrive ---

func TestRedrive_ByIDs_200(t *testing.T) {
	s := newTestServer(t, newDLQStorage())
	rec := doBody(t, s, http.MethodPost, "/v1/queues/q/dlq:redrive", selectorRequest{
		IDs: []string{"id1", "id2"},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", rec.Code, rec.Body)
	}
	var resp countResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Count != 2 {
		t.Errorf("count = %d, want 2", resp.Count)
	}
}

func TestRedrive_ByFilter_FilterPushdown(t *testing.T) {
	st := newDLQStorage()
	st.dlqTasks = []envelope.Envelope{deadEnvelope("id1", "q"), deadEnvelope("id2", "q")}
	st.filterPushdown = true
	s := newTestServer(t, st)

	f := spi.DLQFilter{HandlerRef: "my-handler"}
	rec := doBody(t, s, http.MethodPost, "/v1/queues/q/dlq:redrive", selectorRequest{Filter: &f})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", rec.Code, rec.Body)
	}
	var resp countResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Count != 2 {
		t.Errorf("count = %d, want 2", resp.Count)
	}
	// FilterPushdown: filter passed directly to Redrive, no ID collection.
	if st.lastRedriveIDs != nil {
		t.Errorf("filterPushdown should not convert to IDs; got %v", st.lastRedriveIDs)
	}
}

func TestRedrive_ByFilter_Streaming(t *testing.T) {
	st := newDLQStorage()
	st.dlqTasks = []envelope.Envelope{deadEnvelope("id1", "q"), deadEnvelope("id2", "q")}
	st.filterPushdown = false // triggers streaming path
	s := newTestServer(t, st)

	f := spi.DLQFilter{HandlerRef: "my-handler"}
	rec := doBody(t, s, http.MethodPost, "/v1/queues/q/dlq:redrive", selectorRequest{Filter: &f})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", rec.Code, rec.Body)
	}
	var resp countResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Count != 2 {
		t.Errorf("count = %d, want 2", resp.Count)
	}
	// Streaming path: Redrive must have been called with the collected IDs.
	if len(st.lastRedriveIDs) != 2 {
		t.Errorf("streaming path: lastRedriveIDs = %v, want 2 ids", st.lastRedriveIDs)
	}
}

func TestRedrive_503_NoStorage(t *testing.T) {
	s := New()
	rec := doBody(t, s, http.MethodPost, "/v1/queues/q/dlq:redrive", selectorRequest{})
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func TestRedrive_422_BothIDsAndFilter(t *testing.T) {
	s := newTestServer(t, newDLQStorage())
	f := spi.DLQFilter{ErrorType: "TIMEOUT"}
	rec := doBody(t, s, http.MethodPost, "/v1/queues/q/dlq:redrive", selectorRequest{
		IDs:    []string{"id1"},
		Filter: &f,
	})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", rec.Code)
	}
	if p := decodeProblem(t, rec); p.Code != CodeInvalidTask {
		t.Errorf("code = %q, want INVALID_TASK", p.Code)
	}
}

// --- purge ---

func TestPurge_ByIDs_200(t *testing.T) {
	s := newTestServer(t, newDLQStorage())
	rec := doBody(t, s, http.MethodPost, "/v1/queues/q/dlq:purge", selectorRequest{
		IDs: []string{"id1"},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", rec.Code, rec.Body)
	}
	var resp countResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Count != 1 {
		t.Errorf("count = %d, want 1", resp.Count)
	}
}

func TestPurge_ByFilter_Streaming(t *testing.T) {
	st := newDLQStorage()
	st.dlqTasks = []envelope.Envelope{deadEnvelope("id1", "q")}
	st.filterPushdown = false
	s := newTestServer(t, st)

	f := spi.DLQFilter{ErrorType: "TIMEOUT"}
	rec := doBody(t, s, http.MethodPost, "/v1/queues/q/dlq:purge", selectorRequest{Filter: &f})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", rec.Code, rec.Body)
	}
	var resp countResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Count != 1 {
		t.Errorf("count = %d, want 1", resp.Count)
	}
	if len(st.lastPurgeIDs) != 1 || st.lastPurgeIDs[0] != "id1" {
		t.Errorf("streaming path: lastPurgeIDs = %v, want [id1]", st.lastPurgeIDs)
	}
}

func TestPurge_503_NoStorage(t *testing.T) {
	s := New()
	rec := doBody(t, s, http.MethodPost, "/v1/queues/q/dlq:purge", selectorRequest{})
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

// --- stats ---

func TestStats_200(t *testing.T) {
	st := newDLQStorage()
	st.statsResult = spi.Stats{
		Pending:          3,
		InFlight:         1,
		DLQDepth:         5,
		OldestPendingAge: 2 * time.Second,
	}
	s := newTestServer(t, st)
	rec := do(t, s, http.MethodGet, "/v1/queues/q/stats")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", rec.Code, rec.Body)
	}
	var resp statsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Pending != 3 {
		t.Errorf("pending = %d, want 3", resp.Pending)
	}
	if resp.InFlight != 1 {
		t.Errorf("in_flight = %d, want 1", resp.InFlight)
	}
	if resp.DLQDepth != 5 {
		t.Errorf("dlq_depth = %d, want 5", resp.DLQDepth)
	}
	if resp.OldestPendingAgeMs != 2000 {
		t.Errorf("oldest_pending_age_ms = %d, want 2000", resp.OldestPendingAgeMs)
	}
}

func TestStats_503_NoStorage(t *testing.T) {
	s := New()
	rec := do(t, s, http.MethodGet, "/v1/queues/q/stats")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

// --- pause / resume ---

func TestPause_204(t *testing.T) {
	s := newTestServer(t, newDLQStorage())
	rec := do(t, s, http.MethodPost, "/v1/admin/queues/my-queue:pause")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204\nbody: %s", rec.Code, rec.Body)
	}
	if !s.IsPaused("my-queue") {
		t.Error("queue should be paused after :pause")
	}
}

func TestResume_204(t *testing.T) {
	s := newTestServer(t, newDLQStorage())
	do(t, s, http.MethodPost, "/v1/admin/queues/my-queue:pause")
	if !s.IsPaused("my-queue") {
		t.Fatal("pause did not take effect")
	}
	rec := do(t, s, http.MethodPost, "/v1/admin/queues/my-queue:resume")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204\nbody: %s", rec.Code, rec.Body)
	}
	if s.IsPaused("my-queue") {
		t.Error("queue should not be paused after :resume")
	}
}

// TestPause_SubmitStillAccepted: pause stops claiming but submits are still
// accepted (design 04 §2 — the ops brake accumulates durably).
func TestPause_SubmitStillAccepted(t *testing.T) {
	st := newDLQStorage()
	s := newTestServer(t, st)

	rec := do(t, s, http.MethodPost, "/v1/admin/queues/q:pause")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("pause: status = %d, want 204", rec.Code)
	}

	submitRec := doBody(t, s, http.MethodPost, "/v1/queues/q/tasks", submitTaskRequest{
		HandlerRef: "h", Payload: []byte("x"), PayloadContentType: "text/plain",
	})
	if submitRec.Code != http.StatusAccepted {
		t.Fatalf("submit after pause: status = %d, want 202\nbody: %s", submitRec.Code, submitRec.Body)
	}
}

func TestPause_WrongMethod_405(t *testing.T) {
	s := newTestServer(t, newDLQStorage())
	rec := do(t, s, http.MethodGet, "/v1/admin/queues/q:pause")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
	if p := decodeProblem(t, rec); p.Code != CodeMethodNotAllowed {
		t.Errorf("code = %q, want METHOD_NOT_ALLOWED", p.Code)
	}
}

func TestAdminQueues_UnknownAction_404(t *testing.T) {
	s := newTestServer(t, newDLQStorage())
	rec := do(t, s, http.MethodPost, "/v1/admin/queues/q:unknown")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}
