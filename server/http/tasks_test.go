// SPDX-License-Identifier: Apache-2.0

package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/spi"
)

// fakeStorage is a minimal in-memory spi.Storage for handler tests. Only
// Enqueue and Get are implemented; all other methods are stubs.
type fakeStorage struct {
	mu    sync.Mutex
	tasks map[string]envelope.Envelope
}

func newFakeStorage() *fakeStorage {
	return &fakeStorage{tasks: make(map[string]envelope.Envelope)}
}

func (f *fakeStorage) Enqueue(_ context.Context, task envelope.Envelope) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if existing, ok := f.tasks[task.ID]; ok {
		if existing.Queue != task.Queue {
			return spi.ErrIDConflict
		}
		return nil // idempotent same-queue re-enqueue
	}
	f.tasks[task.ID] = task
	return nil
}

func (f *fakeStorage) Get(_ context.Context, id spi.TaskID) (envelope.Envelope, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	task, ok := f.tasks[id]
	if !ok {
		return envelope.Envelope{}, spi.ErrNotFound
	}
	return task, nil
}

// Unused spi.Storage methods — stubs so the interface is satisfied.
func (f *fakeStorage) ClaimDue(_ context.Context, _ string, _ int, _ time.Duration) ([]spi.Claimed, error) {
	return nil, nil
}
func (f *fakeStorage) ExtendLease(_ context.Context, _ spi.TaskID, _ spi.ClaimToken, _ time.Duration) error {
	return nil
}
func (f *fakeStorage) Reschedule(_ context.Context, _ spi.TaskID, _ spi.ClaimToken, _ spi.Attempt, _ time.Time) error {
	return nil
}
func (f *fakeStorage) Complete(_ context.Context, _ spi.TaskID, _ spi.ClaimToken, _ spi.Attempt) error {
	return nil
}
func (f *fakeStorage) DeadLetter(_ context.Context, _ spi.TaskID, _ spi.ClaimToken, _ spi.Attempt) error {
	return nil
}
func (f *fakeStorage) DLQList(_ context.Context, _ string, _ spi.DLQFilter, _ spi.Page) ([]envelope.Envelope, spi.Cursor, error) {
	return nil, "", nil
}
func (f *fakeStorage) Redrive(_ context.Context, _ string, _ spi.Selector) (int, error) { return 0, nil }
func (f *fakeStorage) Purge(_ context.Context, _ string, _ spi.Selector) (int, error)   { return 0, nil }
func (f *fakeStorage) Stats(_ context.Context, _ string) (spi.Stats, error)              { return spi.Stats{}, nil }
func (f *fakeStorage) PurgeSucceeded(_ context.Context, _ string, _ time.Time) (int, error) {
	return 0, nil
}
func (f *fakeStorage) Capabilities() spi.Capabilities { return spi.Capabilities{} }

// doBody routes a request with a JSON body through the server.
func doBody(t *testing.T, s *Server, method, target string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatalf("encode body: %v", err)
	}
	req := httptest.NewRequest(method, target, &buf)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

// decodeEnvelope decodes the response body as an envelope.Envelope, failing the
// test if it is not valid JSON.
func decodeEnvelope(t *testing.T, rec *httptest.ResponseRecorder) envelope.Envelope {
	t.Helper()
	var env envelope.Envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("decode envelope: %v\nbody: %s", err, rec.Body)
	}
	return env
}

// validSubmit returns a submitTaskRequest with all required fields populated.
func validSubmit(queue string) submitTaskRequest {
	return submitTaskRequest{
		HandlerRef:         "my-handler",
		Payload:            []byte("hello"),
		PayloadContentType: "text/plain",
	}
}

func newTestServer(t *testing.T, st spi.Storage) *Server {
	t.Helper()
	return New(WithStorage(st))
}

// --- submit single task ---

func TestSubmitTask_202(t *testing.T) {
	st := newFakeStorage()
	s := newTestServer(t, st)
	req := validSubmit("my-queue")
	rec := doBody(t, s, http.MethodPost, "/v1/queues/my-queue/tasks", req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202\nbody: %s", rec.Code, rec.Body)
	}
	env := decodeEnvelope(t, rec)
	if env.Queue != "my-queue" {
		t.Errorf("queue = %q, want my-queue", env.Queue)
	}
	if env.HandlerRef != "my-handler" {
		t.Errorf("handler_ref = %q, want my-handler", env.HandlerRef)
	}
	if env.ID == "" {
		t.Error("id must not be empty")
	}
}

func TestSubmitTask_ExplicitID(t *testing.T) {
	st := newFakeStorage()
	s := newTestServer(t, st)
	const explicitID = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	req := submitTaskRequest{
		ID:                 explicitID,
		HandlerRef:         "my-handler",
		Payload:            []byte("data"),
		PayloadContentType: "application/octet-stream",
	}
	rec := doBody(t, s, http.MethodPost, "/v1/queues/my-queue/tasks", req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202\nbody: %s", rec.Code, rec.Body)
	}
	env := decodeEnvelope(t, rec)
	if env.ID != explicitID {
		t.Errorf("id = %q, want %q", env.ID, explicitID)
	}
}

// TestSubmitTask_Idempotency: resubmitting the same id+queue is a no-op that
// returns the EXISTING envelope (design 04 §1).
func TestSubmitTask_Idempotency(t *testing.T) {
	st := newFakeStorage()
	s := newTestServer(t, st)
	const explicitID = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	req := submitTaskRequest{
		ID:                 explicitID,
		HandlerRef:         "my-handler",
		Payload:            []byte("data"),
		PayloadContentType: "application/octet-stream",
	}

	// First submit.
	rec1 := doBody(t, s, http.MethodPost, "/v1/queues/my-queue/tasks", req)
	if rec1.Code != http.StatusAccepted {
		t.Fatalf("first submit: status = %d", rec1.Code)
	}

	// Second submit — same id, same queue.
	rec2 := doBody(t, s, http.MethodPost, "/v1/queues/my-queue/tasks", req)
	if rec2.Code != http.StatusAccepted {
		t.Fatalf("second submit: status = %d, want 202\nbody: %s", rec2.Code, rec2.Body)
	}
	env := decodeEnvelope(t, rec2)
	if env.ID != explicitID {
		t.Errorf("re-submit id = %q, want %q", env.ID, explicitID)
	}
}

func TestSubmitTask_IDConflict(t *testing.T) {
	st := newFakeStorage()
	s := newTestServer(t, st)
	const id = "01ARZ3NDEKTSV4RRFFQ69G5FAV"

	// Enqueue on queue-a.
	rec1 := doBody(t, s, http.MethodPost, "/v1/queues/queue-a/tasks", submitTaskRequest{
		ID: id, HandlerRef: "h", Payload: []byte("x"), PayloadContentType: "text/plain",
	})
	if rec1.Code != http.StatusAccepted {
		t.Fatalf("first submit: status = %d", rec1.Code)
	}

	// Reuse same id on queue-b → 409 ID_CONFLICT.
	rec2 := doBody(t, s, http.MethodPost, "/v1/queues/queue-b/tasks", submitTaskRequest{
		ID: id, HandlerRef: "h", Payload: []byte("x"), PayloadContentType: "text/plain",
	})
	if rec2.Code != http.StatusConflict {
		t.Fatalf("cross-queue submit: status = %d, want 409\nbody: %s", rec2.Code, rec2.Body)
	}
	if p := decodeProblem(t, rec2); p.Code != CodeIDConflict {
		t.Errorf("code = %q, want ID_CONFLICT", p.Code)
	}
}

// --- submit rejections ---

func TestSubmitTask_422_MissingHandlerRef(t *testing.T) {
	s := newTestServer(t, newFakeStorage())
	req := submitTaskRequest{PayloadContentType: "text/plain", Payload: []byte("x")}
	rec := doBody(t, s, http.MethodPost, "/v1/queues/q/tasks", req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", rec.Code)
	}
	if p := decodeProblem(t, rec); p.Code != CodeInvalidTask {
		t.Errorf("code = %q, want INVALID_TASK", p.Code)
	}
}

func TestSubmitTask_422_MissingContentType(t *testing.T) {
	s := newTestServer(t, newFakeStorage())
	req := submitTaskRequest{HandlerRef: "h", Payload: []byte("x")}
	rec := doBody(t, s, http.MethodPost, "/v1/queues/q/tasks", req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", rec.Code)
	}
	if p := decodeProblem(t, rec); p.Code != CodeInvalidTask {
		t.Errorf("code = %q, want INVALID_TASK", p.Code)
	}
}

func TestSubmitTask_413_PayloadTooLarge(t *testing.T) {
	// Configure a 10-byte limit so the test doesn't need a huge payload.
	s := New(WithStorage(newFakeStorage()), WithMaxPayloadBytes(10))
	req := submitTaskRequest{
		HandlerRef:         "h",
		Payload:            []byte(strings.Repeat("x", 11)),
		PayloadContentType: "text/plain",
	}
	rec := doBody(t, s, http.MethodPost, "/v1/queues/q/tasks", req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413\nbody: %s", rec.Code, rec.Body)
	}
	if p := decodeProblem(t, rec); p.Code != CodePayloadTooLarge {
		t.Errorf("code = %q, want PAYLOAD_TOO_LARGE", p.Code)
	}
}

func TestSubmitTask_422_InvalidJSON(t *testing.T) {
	s := newTestServer(t, newFakeStorage())
	req := httptest.NewRequest(http.MethodPost, "/v1/queues/q/tasks", strings.NewReader("{bad json"))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", rec.Code)
	}
	if p := decodeProblem(t, rec); p.Code != CodeInvalidTask {
		t.Errorf("code = %q, want INVALID_TASK", p.Code)
	}
}

func TestSubmitTask_503_NoStorage(t *testing.T) {
	s := New() // no storage configured
	req := validSubmit("q")
	rec := doBody(t, s, http.MethodPost, "/v1/queues/q/tasks", req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

// --- batch ---

func TestBatchSubmit_207_PerItemResults(t *testing.T) {
	st := newFakeStorage()
	s := New(WithStorage(st), WithMaxPayloadBytes(20))

	// Item 0: valid.
	// Item 1: missing handler_ref → 422.
	// Item 2: valid.
	items := []submitTaskRequest{
		{HandlerRef: "h", Payload: []byte("ok"), PayloadContentType: "text/plain"},
		{Payload: []byte("ok"), PayloadContentType: "text/plain"}, // no handler_ref
		{HandlerRef: "h", Payload: []byte("ok2"), PayloadContentType: "text/plain"},
	}
	rec := doBody(t, s, http.MethodPost, "/v1/queues/q/tasks:batch", items)
	if rec.Code != http.StatusMultiStatus {
		t.Fatalf("status = %d, want 207\nbody: %s", rec.Code, rec.Body)
	}

	var results []batchItemResult
	if err := json.Unmarshal(rec.Body.Bytes(), &results); err != nil {
		t.Fatalf("decode results: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}

	// Index 0: success
	if results[0].Index != 0 || results[0].Status != http.StatusAccepted || results[0].Envelope == nil {
		t.Errorf("item 0: %+v", results[0])
	}
	// Index 1: 422
	if results[1].Index != 1 || results[1].Status != http.StatusUnprocessableEntity || results[1].Error == nil {
		t.Errorf("item 1: %+v", results[1])
	}
	if results[1].Error.Code != CodeInvalidTask {
		t.Errorf("item 1 error code = %q, want INVALID_TASK", results[1].Error.Code)
	}
	// Index 2: success
	if results[2].Index != 2 || results[2].Status != http.StatusAccepted || results[2].Envelope == nil {
		t.Errorf("item 2: %+v", results[2])
	}
}

func TestBatchSubmit_207_IdempotentItem(t *testing.T) {
	st := newFakeStorage()
	s := newTestServer(t, st)
	const id = "01ARZ3NDEKTSV4RRFFQ69G5FAV"

	// First batch containing the explicit id.
	items := []submitTaskRequest{{
		ID: id, HandlerRef: "h", Payload: []byte("x"), PayloadContentType: "text/plain",
	}}
	rec1 := doBody(t, s, http.MethodPost, "/v1/queues/q/tasks:batch", items)
	if rec1.Code != http.StatusMultiStatus {
		t.Fatalf("first batch: %d", rec1.Code)
	}

	// Second batch with same id → still 202 per item (idempotent re-submit).
	rec2 := doBody(t, s, http.MethodPost, "/v1/queues/q/tasks:batch", items)
	if rec2.Code != http.StatusMultiStatus {
		t.Fatalf("second batch: %d", rec2.Code)
	}
	var results []batchItemResult
	if err := json.Unmarshal(rec2.Body.Bytes(), &results); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if results[0].Status != http.StatusAccepted {
		t.Errorf("re-submit item status = %d, want 202", results[0].Status)
	}
}

func TestBatchSubmit_422_InvalidJSON(t *testing.T) {
	s := newTestServer(t, newFakeStorage())
	req := httptest.NewRequest(http.MethodPost, "/v1/queues/q/tasks:batch", strings.NewReader("not-json"))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", rec.Code)
	}
}

func TestBatchSubmit_503_NoStorage(t *testing.T) {
	s := New()
	rec := doBody(t, s, http.MethodPost, "/v1/queues/q/tasks:batch", []submitTaskRequest{})
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

// --- get task ---

func TestGetTask_200(t *testing.T) {
	st := newFakeStorage()
	s := newTestServer(t, st)

	// Enqueue a task first.
	req := submitTaskRequest{
		HandlerRef:         "h",
		Payload:            []byte("pay"),
		PayloadContentType: "text/plain",
	}
	rec := doBody(t, s, http.MethodPost, "/v1/queues/my-queue/tasks", req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("submit: %d", rec.Code)
	}
	env := decodeEnvelope(t, rec)

	// Fetch it.
	getRec := do(t, s, http.MethodGet, "/v1/tasks/"+env.ID)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get status = %d, want 200\nbody: %s", getRec.Code, getRec.Body)
	}
	fetched := decodeEnvelope(t, getRec)
	if fetched.ID != env.ID {
		t.Errorf("fetched id = %q, want %q", fetched.ID, env.ID)
	}
	if fetched.Queue != "my-queue" {
		t.Errorf("fetched queue = %q, want my-queue", fetched.Queue)
	}
}

func TestGetTask_404(t *testing.T) {
	s := newTestServer(t, newFakeStorage())
	rec := do(t, s, http.MethodGet, "/v1/tasks/01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if p := decodeProblem(t, rec); p.Code != CodeNotFound {
		t.Errorf("code = %q, want NOT_FOUND", p.Code)
	}
}

func TestGetTask_503_NoStorage(t *testing.T) {
	s := New()
	rec := do(t, s, http.MethodGet, "/v1/tasks/01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}
