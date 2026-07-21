// SPDX-License-Identifier: Apache-2.0

package http

import (
	"encoding/json"
	"net/http"
	"testing"

	coreconfig "github.com/srjn45/rdq/core/config"
	srvconfig "github.com/srjn45/rdq/server/config"
)

// newAdminServer constructs a Server with both storage and a fresh MemStore.
func newAdminServer(t *testing.T) (*Server, *srvconfig.MemStore) {
	t.Helper()
	cs := srvconfig.NewMemStore()
	s := New(WithStorage(newDLQStorage()), WithConfigStore(cs))
	return s, cs
}

// validQueueConfig returns a minimal valid QueueConfig for use in PUT tests.
func validQueueConfig() coreconfig.QueueConfig {
	ma := 3
	return coreconfig.QueueConfig{
		Retry: &coreconfig.RetryConfig{MaxAttempts: &ma},
	}
}

// --- list queues ---

func TestListQueues_200_Empty(t *testing.T) {
	s, _ := newAdminServer(t)
	rec := do(t, s, http.MethodGet, "/v1/admin/queues")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", rec.Code, rec.Body)
	}
	var resp []queueSummary
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp) != 0 {
		t.Errorf("queues = %d, want 0", len(resp))
	}
}

func TestListQueues_200_WithQueues(t *testing.T) {
	s, cs := newAdminServer(t)
	qc := validQueueConfig()
	_ = cs.Put("payments.charge", &qc)
	_ = cs.Put("orders.process", &qc)

	rec := do(t, s, http.MethodGet, "/v1/admin/queues")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", rec.Code, rec.Body)
	}
	var resp []queueSummary
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp) != 2 {
		t.Fatalf("queues = %d, want 2", len(resp))
	}
	// Sorted order: orders.process before payments.charge.
	if resp[0].Queue != "orders.process" {
		t.Errorf("resp[0].queue = %q, want orders.process", resp[0].Queue)
	}
	if resp[1].Queue != "payments.charge" {
		t.Errorf("resp[1].queue = %q, want payments.charge", resp[1].Queue)
	}
}

func TestListQueues_PausedReflected(t *testing.T) {
	s, cs := newAdminServer(t)
	qc := validQueueConfig()
	_ = cs.Put("my-queue", &qc)
	_ = cs.SetPaused("my-queue", true)

	rec := do(t, s, http.MethodGet, "/v1/admin/queues")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", rec.Code, rec.Body)
	}
	var resp []queueSummary
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp) != 1 {
		t.Fatalf("queues = %d, want 1", len(resp))
	}
	if !resp[0].Paused {
		t.Error("want paused=true, got false")
	}
}

func TestListQueues_503_NoConfigStore(t *testing.T) {
	s := New()
	rec := do(t, s, http.MethodGet, "/v1/admin/queues")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

// --- GET config ---

func TestGetQueueConfig_200(t *testing.T) {
	s, cs := newAdminServer(t)
	qc := validQueueConfig()
	_ = cs.Put("payments.charge", &qc)

	rec := do(t, s, http.MethodGet, "/v1/admin/queues/payments.charge/config")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", rec.Code, rec.Body)
	}
	var resp queueConfigResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Queue != "payments.charge" {
		t.Errorf("queue = %q, want payments.charge", resp.Queue)
	}
	if resp.Config == nil {
		t.Fatal("config must not be nil")
	}
}

func TestGetQueueConfig_404_NotConfigured(t *testing.T) {
	s, _ := newAdminServer(t)
	rec := do(t, s, http.MethodGet, "/v1/admin/queues/nonexistent/config")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if p := decodeProblem(t, rec); p.Code != CodeQueueNotFound {
		t.Errorf("code = %q, want QUEUE_NOT_FOUND", p.Code)
	}
}

func TestGetQueueConfig_503_NoConfigStore(t *testing.T) {
	s := New()
	rec := do(t, s, http.MethodGet, "/v1/admin/queues/q/config")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func TestQueueConfig_405_WrongMethod(t *testing.T) {
	s, _ := newAdminServer(t)
	rec := do(t, s, http.MethodPost, "/v1/admin/queues/q/config")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
	if got := rec.Header().Get("Allow"); got == "" {
		t.Error("Allow header missing on 405")
	}
}

// --- PUT config ---

func TestPutQueueConfig_200_Create(t *testing.T) {
	s, cs := newAdminServer(t)
	qc := validQueueConfig()
	rec := doBody(t, s, http.MethodPut, "/v1/admin/queues/my-queue/config", qc)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", rec.Code, rec.Body)
	}
	// Verify it was stored.
	entry, err := cs.Get("my-queue")
	if err != nil {
		t.Fatalf("config not stored: %v", err)
	}
	if entry.Config == nil {
		t.Error("stored config must not be nil")
	}
}

func TestPutQueueConfig_200_Update(t *testing.T) {
	s, cs := newAdminServer(t)
	qc := validQueueConfig()
	_ = cs.Put("my-queue", &qc)

	ma := 10
	updated := coreconfig.QueueConfig{Retry: &coreconfig.RetryConfig{MaxAttempts: &ma}}
	rec := doBody(t, s, http.MethodPut, "/v1/admin/queues/my-queue/config", updated)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", rec.Code, rec.Body)
	}
	entry, err := cs.Get("my-queue")
	if err != nil {
		t.Fatalf("config not found: %v", err)
	}
	if *entry.Config.Retry.MaxAttempts != 10 {
		t.Errorf("max_attempts = %d, want 10", *entry.Config.Retry.MaxAttempts)
	}
}

// TestPutQueueConfig_NextClaimEffect asserts that a PUT config takes effect on
// the next Get (which the claim loop calls), satisfying the acceptance criterion.
func TestPutQueueConfig_NextClaimEffect(t *testing.T) {
	s, cs := newAdminServer(t)

	// Initial state: no config.
	if _, err := cs.Get("payments.charge"); err == nil {
		t.Fatal("expected ErrNotFound before PUT")
	}

	// PUT new config.
	ma := 5
	qc := coreconfig.QueueConfig{Retry: &coreconfig.RetryConfig{MaxAttempts: &ma}}
	rec := doBody(t, s, http.MethodPut, "/v1/admin/queues/payments.charge/config", qc)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", rec.Code, rec.Body)
	}

	// After PUT the config store immediately reflects the change (claim reads here).
	entry, err := cs.Get("payments.charge")
	if err != nil {
		t.Fatalf("config not found after PUT: %v", err)
	}
	if *entry.Config.Retry.MaxAttempts != 5 {
		t.Errorf("max_attempts = %d, want 5", *entry.Config.Retry.MaxAttempts)
	}
}

func TestPutQueueConfig_422_InvalidConfig(t *testing.T) {
	s, _ := newAdminServer(t)
	// max_attempts = 0 is invalid (must be >= 1).
	ma := 0
	qc := coreconfig.QueueConfig{Retry: &coreconfig.RetryConfig{MaxAttempts: &ma}}
	rec := doBody(t, s, http.MethodPut, "/v1/admin/queues/my-queue/config", qc)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422\nbody: %s", rec.Code, rec.Body)
	}
	if p := decodeProblem(t, rec); p.Code != CodeInvalidTask {
		t.Errorf("code = %q, want INVALID_TASK", p.Code)
	}
}

func TestPutQueueConfig_422_UnknownField(t *testing.T) {
	s, _ := newAdminServer(t)
	// Send a JSON body with an unknown field — strict validation must reject it.
	raw := map[string]any{"unknown_field": "oops", "retry": map[string]any{"max_attempts": 3}}
	rec := doBody(t, s, http.MethodPut, "/v1/admin/queues/my-queue/config", raw)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422\nbody: %s", rec.Code, rec.Body)
	}
}

func TestPutQueueConfig_503_NoConfigStore(t *testing.T) {
	s := New()
	qc := validQueueConfig()
	rec := doBody(t, s, http.MethodPut, "/v1/admin/queues/q/config", qc)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

// --- DELETE queue ---

func TestDeleteQueue_204_Empty(t *testing.T) {
	s, cs := newAdminServer(t)
	qc := validQueueConfig()
	_ = cs.Put("empty-queue", &qc)

	rec := do(t, s, http.MethodDelete, "/v1/admin/queues/empty-queue")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204\nbody: %s", rec.Code, rec.Body)
	}
	// Verify removed from store.
	if _, err := cs.Get("empty-queue"); err == nil {
		t.Error("queue still present in config store after delete")
	}
}

// TestDeleteQueue_409_NonEmpty asserts the acceptance criterion:
// delete of a non-empty queue returns 409 CONFLICT.
func TestDeleteQueue_409_NonEmpty(t *testing.T) {
	// Use a dlqStorage whose Stats returns non-zero pending count.
	st := newDLQStorage()
	st.statsResult.Pending = 3
	cs := srvconfig.NewMemStore()
	qc := validQueueConfig()
	_ = cs.Put("busy-queue", &qc)
	s := New(WithStorage(st), WithConfigStore(cs))

	rec := do(t, s, http.MethodDelete, "/v1/admin/queues/busy-queue")
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409\nbody: %s", rec.Code, rec.Body)
	}
	if p := decodeProblem(t, rec); p.Code != CodeConflict {
		t.Errorf("code = %q, want CONFLICT", p.Code)
	}
	// Queue must still exist in config store.
	if _, err := cs.Get("busy-queue"); err != nil {
		t.Error("queue should still be in config store after rejected delete")
	}
}

func TestDeleteQueue_409_NonEmpty_DLQ(t *testing.T) {
	st := newDLQStorage()
	st.statsResult.DLQDepth = 1
	cs := srvconfig.NewMemStore()
	qc := validQueueConfig()
	_ = cs.Put("q", &qc)
	s := New(WithStorage(st), WithConfigStore(cs))

	rec := do(t, s, http.MethodDelete, "/v1/admin/queues/q")
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409, body: %s", rec.Code, rec.Body)
	}
}

func TestDeleteQueue_404_NotConfigured(t *testing.T) {
	s, _ := newAdminServer(t)
	rec := do(t, s, http.MethodDelete, "/v1/admin/queues/nonexistent")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if p := decodeProblem(t, rec); p.Code != CodeQueueNotFound {
		t.Errorf("code = %q, want QUEUE_NOT_FOUND", p.Code)
	}
}

func TestDeleteQueue_503_NoConfigStore(t *testing.T) {
	s := New()
	rec := do(t, s, http.MethodDelete, "/v1/admin/queues/q")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

// --- pause state persisted in ConfigStore ---

// TestPausePersistsInConfigStore: after pause, IsPaused reads from the
// ConfigStore so it survives a server restart (design 04 §2).
func TestPausePersistsInConfigStore(t *testing.T) {
	cs := srvconfig.NewMemStore()
	s1 := New(WithStorage(newDLQStorage()), WithConfigStore(cs))

	rec := do(t, s1, http.MethodPost, "/v1/admin/queues/my-queue:pause")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("pause: status = %d, want 204", rec.Code)
	}

	// Simulate restart: create a new Server backed by the SAME store.
	s2 := New(WithStorage(newDLQStorage()), WithConfigStore(cs))
	if !s2.IsPaused("my-queue") {
		t.Error("IsPaused must return true after restart when ConfigStore is used")
	}
}

func TestResumeRemovesPauseFromConfigStore(t *testing.T) {
	cs := srvconfig.NewMemStore()
	_ = cs.SetPaused("my-queue", true)
	s := New(WithStorage(newDLQStorage()), WithConfigStore(cs))

	rec := do(t, s, http.MethodPost, "/v1/admin/queues/my-queue:resume")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("resume: status = %d, want 204", rec.Code)
	}
	if cs.IsPaused("my-queue") {
		t.Error("ConfigStore should show not-paused after resume")
	}
}
