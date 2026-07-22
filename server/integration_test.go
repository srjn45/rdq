// SPDX-License-Identifier: Apache-2.0

package server

// This is the T5.7 cross-process integration test: it wires the REAL rdq-server
// data plane (server/http), the callback dispatcher (server/callback, T5.5), the
// auth boundary (server/auth + server config, T5.6), and the engine worker loop
// (core/engine) on top of a Postgres testcontainer (T2.6), then drives the FULL
// lifecycle over loopback HTTP:
//
//	submit (HTTP API) -> engine claims -> HTTP callback dispatch -> receiver
//	fails -> retries exhaust -> dead-letter -> redrive (DLQ HTTP API) -> callback
//	succeeds -> terminal SUCCEEDED
//
// It is genuinely cross-process in shape: the API server runs on a real
// net.Listener (httptest) served in its own goroutine, the worker runs in its
// own goroutine against the same Postgres, and the callback receiver is a
// separate loopback http.Server. The test is only a *driver* — every component
// it exercises is production code. There is no production binary that wires
// submit->engine->callback end to end yet (that is the server main), so the
// small engine<->callback bridge handler here plays that role for the test.
//
// Auth (T5.6) is exercised positively (submitter submits, operator redrives) and
// negatively (no token -> 401, submitter cannot redrive -> 403). The callback
// allowlist (T5.6 SSRF boundary) is exercised positively (the receiver's host is
// on the list and delivery happens) and negatively (an off-allowlist URL is
// rejected at config validation).
//
// Memory: exactly ONE Postgres container for the whole test, and minimal
// attempt/retry counts (max_attempts=2), per the run's tight-swap constraint.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	coreconfig "github.com/srjn45/rdq/core/config"
	"github.com/srjn45/rdq/core/engine"
	"github.com/srjn45/rdq/core/envelope"
	"github.com/srjn45/rdq/core/policy"
	"github.com/srjn45/rdq/core/registry"
	"github.com/srjn45/rdq/server/auth"
	"github.com/srjn45/rdq/server/callback"
	srvconfig "github.com/srjn45/rdq/server/config"
	srvhttp "github.com/srjn45/rdq/server/http"
	"github.com/srjn45/rdq/storage/postgres"

	"github.com/testcontainers/testcontainers-go"
	pgmod "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// Integration test knobs, kept minimal to prove the loop without OOMing a
// tight-swap host: two attempts is the smallest count that exhausts a retry
// (attempt 1 fails -> reschedule -> attempt 2 fails -> dead-letter), and a tiny
// backoff keeps the retry prompt so the whole loop finishes in seconds.
const (
	itQueue      = "payments.callbacks"
	itHandlerRef = "cb.echo"
	itMaxAtt     = 2
	itBackoff    = 40 * time.Millisecond
	itLease      = 5 * time.Second
	itPoll       = 100 * time.Millisecond
	itCBTimeout  = 2 * time.Second

	// Tokens for the two principals the test drives the API as.
	tokSubmitter = "tok-submitter-secret"
	tokOperator  = "tok-operator-secret"

	// The callback receiver's shared secret, delivered via secret_ref env
	// indirection (T5.6) and verified by the receiver on every delivery.
	cbSecretEnv = "RDQ_TEST_CB_SECRET"
	cbSecret    = "cb-shared-secret-abc123"

	waitTimeout = 45 * time.Second
)

// TestIntegration_SubmitCallbackRetryDLQRedrive is the T5.7 acceptance test.
func TestIntegration_SubmitCallbackRetryDLQRedrive(t *testing.T) {
	ctx := context.Background()

	// --- Postgres: ONE container for the whole test (memory constraint). ------
	dsn := startIntegrationPostgres(ctx, t)
	db, err := postgres.Open(dsn)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := postgres.Migrate(ctx, db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	store := postgres.New(db)

	// --- Stub callback receiver on a real loopback port. ----------------------
	// It fails (500) while failing.Load() is true, so the test can force retry
	// exhaustion, then flip it to succeed (200) for the post-redrive delivery.
	recv := &stubReceiver{secret: cbSecret}
	recv.failing.Store(true)
	recvSrv := httptest.NewServer(recv)
	t.Cleanup(recvSrv.Close)
	callbackURL := recvSrv.URL + "/hook"

	// --- Server config: token file + callback allowlist (T5.6). ---------------
	t.Setenv(cbSecretEnv, cbSecret)
	tokensPath := writeTokenFile(t)
	tokenStore, err := auth.LoadTokenStore(tokensPath)
	if err != nil {
		t.Fatalf("load token store: %v", err)
	}
	authz := auth.NewAuthorizer(tokenStore)

	serverCfg := &srvconfig.ServerConfig{
		TokensPath:        tokensPath,
		CallbackAllowlist: []string{recvSrv.URL}, // exact host:port of the receiver
	}

	// --- Queue config, and the two allowlist assertions (positive + SSRF). ----
	qc := integrationQueueConfig(callbackURL)
	env := func(k string) (string, bool) { return os.LookupEnv(k) }

	// Positive: the on-allowlist callback URL validates cleanly (secret_ref too).
	if err := serverCfg.ValidateCallbacks(map[string]*coreconfig.QueueConfig{itQueue: qc}, env); err != nil {
		t.Fatalf("ValidateCallbacks (on-allowlist) should pass: %v", err)
	}
	// Negative: an off-allowlist target (classic SSRF metadata endpoint) is
	// rejected at config load, before any claim loop could dispatch to it.
	ssrf := integrationQueueConfig("http://169.254.169.254/latest/meta-data")
	if err := serverCfg.ValidateCallbacks(map[string]*coreconfig.QueueConfig{itQueue: ssrf}, env); err == nil {
		t.Fatal("ValidateCallbacks (off-allowlist SSRF url) should be rejected, got nil")
	}

	// --- Resolve the callback secret_ref and build the dispatch target. -------
	secret, err := srvconfig.ResolveSecretRef(*qc.Callback.Auth.SecretRef, env)
	if err != nil {
		t.Fatalf("resolve callback secret_ref: %v", err)
	}
	allowlist, err := serverCfg.Allowlist()
	if err != nil {
		t.Fatalf("parse allowlist: %v", err)
	}
	target := callback.Target{
		URL:         callbackURL,
		ContentType: "application/json",
		Timeout:     qc.Callback.Timeout.Std(),
		Auth:        callback.AuthBearer,
		Secret:      secret,
	}

	// --- Engine: registry with the callback-bridge handler + worker loop. -----
	reg := registry.New()
	bridge := &callbackBridge{
		dispatcher: callback.New(),
		target:     target,
		allowlist:  allowlist,
	}
	if err := reg.Register(itHandlerRef, bridge); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	spec, err := engine.SpecFromConfig(itQueue, qc)
	if err != nil {
		t.Fatalf("spec from config: %v", err)
	}
	worker, err := engine.NewWorker(store, reg, []engine.QueueSpec{spec},
		engine.WithSweepInterval(0)) // no sweeper: nothing to retain in this test
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	go func() { defer close(workerDone); _ = worker.Run(workerCtx) }()
	t.Cleanup(func() {
		stopWorker()
		select {
		case <-workerDone:
		case <-time.After(2 * itLease):
			t.Log("worker did not drain within deadline")
		}
	})

	// --- Real API server on a loopback port. ----------------------------------
	apiSrv := srvhttp.New(
		srvhttp.WithStorage(store),
		srvhttp.WithAuthorizer(authz),
	)
	api := httptest.NewServer(apiSrv.Handler())
	t.Cleanup(api.Close)
	cl := &apiClient{base: api.URL, hc: api.Client()}

	// ======================= DRIVE THE FULL LOOP ==============================

	// (1) authN: submitting with no token is 401 UNAUTHENTICATED.
	if code, _ := cl.do(t, http.MethodPost, "/v1/queues/"+itQueue+"/tasks", "", submitBody()); code != http.StatusUnauthorized {
		t.Fatalf("submit without token: status = %d, want 401", code)
	}

	// (2) submit the task as the submitter principal -> 202 Accepted, PENDING.
	code, body := cl.do(t, http.MethodPost, "/v1/queues/"+itQueue+"/tasks", tokSubmitter, submitBody())
	if code != http.StatusAccepted {
		t.Fatalf("submit: status = %d, want 202; body=%s", code, body)
	}
	var submitted envelope.Envelope
	mustJSON(t, body, &submitted)
	taskID := submitted.ID
	if taskID == "" {
		t.Fatal("submit returned an envelope with no id")
	}
	if submitted.Status != envelope.StatusPending {
		t.Fatalf("submitted status = %q, want PENDING", submitted.Status)
	}

	// (3) the receiver is failing: the worker exhausts retries and dead-letters.
	dead := cl.waitForStatus(t, taskID, tokSubmitter, envelope.StatusDead)
	if dead.AttemptCount != itMaxAtt {
		t.Errorf("dead task attempt_count = %d, want %d (all attempts exhausted)", dead.AttemptCount, itMaxAtt)
	}
	if got := recv.count(); got < itMaxAtt {
		t.Errorf("receiver saw %d deliveries before DLQ, want >= %d", got, itMaxAtt)
	}
	if !recv.sawAuthOK() {
		t.Error("receiver never saw a correct Authorization header (callback auth/secret_ref broken)")
	}

	// (4) stats reflect the dead-letter (observable transition via the API).
	if st := cl.stats(t, itQueue, tokSubmitter); st.DLQDepth != 1 {
		t.Errorf("stats.dlq_depth = %d after dead-letter, want 1", st.DLQDepth)
	}

	// (5) the DLQ browse API lists the dead task (operator-only).
	dlq := cl.dlqList(t, itQueue, tokOperator)
	if len(dlq) != 1 || dlq[0].ID != taskID {
		t.Fatalf("dlq list = %+v, want exactly the dead task %s", dlq, taskID)
	}

	// (6) authZ: the submitter principal may NOT redrive (needs operator) -> 403.
	if code, _ := cl.do(t, http.MethodPost, "/v1/queues/"+itQueue+"/dlq:redrive", tokSubmitter, redriveBody(taskID)); code != http.StatusForbidden {
		t.Fatalf("redrive as submitter: status = %d, want 403", code)
	}

	// (7) flip the receiver to succeed, then redrive as the operator principal.
	recv.failing.Store(false)
	before := recv.count()
	code, body = cl.do(t, http.MethodPost, "/v1/queues/"+itQueue+"/dlq:redrive", tokOperator, redriveBody(taskID))
	if code != http.StatusOK {
		t.Fatalf("redrive as operator: status = %d, want 200; body=%s", code, body)
	}
	var redriven struct {
		Count int `json:"count"`
	}
	mustJSON(t, body, &redriven)
	if redriven.Count != 1 {
		t.Fatalf("redrive count = %d, want 1", redriven.Count)
	}

	// (8) the redriven task runs again and this time the callback acks -> SUCCEEDED.
	succeeded := cl.waitForStatus(t, taskID, tokSubmitter, envelope.StatusSucceeded)
	if succeeded.Status != envelope.StatusSucceeded {
		t.Fatalf("final status = %q, want SUCCEEDED", succeeded.Status)
	}
	if recv.count() <= before {
		t.Error("receiver saw no new delivery after redrive")
	}

	// (9) the DLQ is empty again — the loop closed cleanly.
	if st := cl.stats(t, itQueue, tokSubmitter); st.DLQDepth != 0 {
		t.Errorf("stats.dlq_depth = %d after successful redrive, want 0", st.DLQDepth)
	}
}

// ---------------------------------------------------------------------------
// callback-bridge handler: the engine<->callback seam the server main will own.
// ---------------------------------------------------------------------------

// callbackBridge adapts a callback.Dispatcher to a registry.Handler so the
// engine worker delivers each claimed task over HTTP. It re-checks the SSRF
// allowlist at dispatch time (belt-and-suspenders over the config-load check)
// and translates the classified callback Result into an engine decision: a 2xx
// ack is success (nil), a retryable outcome is a policy.Retryable error (another
// attempt), and a permanent outcome is policy.Permanent (immediate dead-letter).
type callbackBridge struct {
	dispatcher *callback.Dispatcher
	target     callback.Target
	allowlist  *srvconfig.Allowlist
}

func (b *callbackBridge) Version() string { return "" }

func (b *callbackBridge) Handle(ctx context.Context, task envelope.Envelope) error {
	if !b.allowlist.Allows(b.target.URL) {
		return policy.Permanent(fmt.Errorf("callback url %q is off the allowlist", b.target.URL))
	}
	res, err := b.dispatcher.Dispatch(ctx, b.target, callback.Task{
		ID:         task.ID,
		Queue:      task.Queue,
		HandlerRef: task.HandlerRef,
		Attempt:    task.AttemptCount + 1,
		Payload:    task.Payload,
		Headers:    task.Headers,
	})
	if err != nil {
		// A build error is a misconfiguration, not a delivery outcome; retry it.
		return policy.Retryable(fmt.Errorf("callback dispatch: %w", err))
	}
	if res.Success() {
		return nil
	}
	e := fmt.Errorf("callback %s (status %d)", res.Outcome, res.Status)
	if res.Outcome == envelope.OutcomePermanentFailure {
		return policy.Permanent(e)
	}
	return policy.Retryable(e)
}

// ---------------------------------------------------------------------------
// stub callback receiver
// ---------------------------------------------------------------------------

// stubReceiver is the loopback callback endpoint. It counts deliveries, verifies
// the bearer credential on every request, and either fails (500) or acks (200)
// per the failing flag the test toggles.
type stubReceiver struct {
	secret  string
	failing atomic.Bool
	calls   atomic.Int64
	authOK  atomic.Bool
}

func (r *stubReceiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.calls.Add(1)
	if req.Header.Get("Authorization") == "Bearer "+r.secret {
		r.authOK.Store(true)
	}
	// Drain the body so the connection can be reused; the payload is echoed back
	// on success purely so a real body round-trips.
	body, _ := io.ReadAll(req.Body)
	if r.failing.Load() {
		http.Error(w, `{"reason":"stub forced failure"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (r *stubReceiver) count() int64    { return r.calls.Load() }
func (r *stubReceiver) sawAuthOK() bool { return r.authOK.Load() }

// ---------------------------------------------------------------------------
// tiny API client + helpers
// ---------------------------------------------------------------------------

type apiClient struct {
	base string
	hc   *http.Client
}

// do issues one request with an optional bearer token and returns the status
// code and response body. A blank token omits the Authorization header.
func (c *apiClient) do(t *testing.T, method, path, token string, body []byte) (int, []byte) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, c.base+path, rdr)
	if err != nil {
		t.Fatalf("build request %s %s: %v", method, path, err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		t.Fatalf("request %s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out
}

// waitForStatus polls GET /v1/tasks/{id} until the task reaches want or the
// deadline elapses, returning the terminal envelope.
func (c *apiClient) waitForStatus(t *testing.T, id, token string, want envelope.Status) envelope.Envelope {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	var last envelope.Envelope
	for time.Now().Before(deadline) {
		code, body := c.do(t, http.MethodGet, "/v1/tasks/"+id, token, nil)
		if code == http.StatusOK {
			mustJSON(t, body, &last)
			if last.Status == want {
				return last
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("task %s never reached %q (last status %q) within %s", id, want, last.Status, waitTimeout)
	return last
}

func (c *apiClient) stats(t *testing.T, queue, token string) statsResp {
	t.Helper()
	code, body := c.do(t, http.MethodGet, "/v1/queues/"+queue+"/stats", token, nil)
	if code != http.StatusOK {
		t.Fatalf("stats: status = %d; body=%s", code, body)
	}
	var st statsResp
	mustJSON(t, body, &st)
	return st
}

func (c *apiClient) dlqList(t *testing.T, queue, token string) []envelope.Envelope {
	t.Helper()
	code, body := c.do(t, http.MethodGet, "/v1/queues/"+queue+"/dlq", token, nil)
	if code != http.StatusOK {
		t.Fatalf("dlq list: status = %d; body=%s", code, body)
	}
	var out struct {
		Tasks []envelope.Envelope `json:"tasks"`
	}
	mustJSON(t, body, &out)
	return out.Tasks
}

type statsResp struct {
	Pending  int64 `json:"pending"`
	InFlight int64 `json:"in_flight"`
	DLQDepth int64 `json:"dlq_depth"`
}

func submitBody() []byte {
	b, _ := json.Marshal(map[string]any{
		"handler_ref":          itHandlerRef,
		"payload":              []byte(`{"amount":100}`),
		"payload_content_type": "application/json",
	})
	return b
}

func redriveBody(id string) []byte {
	b, _ := json.Marshal(map[string]any{"ids": []string{id}})
	return b
}

func mustJSON(t *testing.T, data []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatalf("decode json %s: %v\nbody=%s", fmt.Sprintf("%T", v), err, data)
	}
}

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

// integrationQueueConfig builds a resolved queue config for the callback queue:
// a fully specified retry ladder + lease (required by engine.SpecFromConfig), a
// fast poll, and an HTTP callback with bearer auth via secret_ref indirection.
func integrationQueueConfig(callbackURL string) *coreconfig.QueueConfig {
	return &coreconfig.QueueConfig{
		Retry: &coreconfig.RetryConfig{
			MaxAttempts:       intPtr(itMaxAtt),
			InitialBackoff:    durPtr(itBackoff),
			BackoffMultiplier: f64Ptr(1.0),
			MaxBackoff:        durPtr(itBackoff),
			Jitter:            f64Ptr(0),
		},
		Execution: &coreconfig.ExecutionConfig{
			Lease: durPtr(itLease),
		},
		Worker: &coreconfig.WorkerConfig{
			PollInterval: durPtr(itPoll),
			BatchSize:    intPtr(1),
			Concurrency:  intPtr(1),
		},
		Callback: &coreconfig.CallbackConfig{
			Protocol: strPtr("http"),
			URL:      strPtr(callbackURL),
			Timeout:  durPtr(itCBTimeout),
			Auth: &coreconfig.CallbackAuth{
				Type:      strPtr("bearer"),
				SecretRef: strPtr("env:" + cbSecretEnv),
			},
		},
	}
}

// writeTokenFile writes a static token file with two principals — a submitter
// and an operator, both scoped to payments.* — and returns its path.
func writeTokenFile(t *testing.T) string {
	t.Helper()
	doc := fmt.Sprintf(`{
	  "principals": [
	    {"name": "submitter", "token": %q, "grants": [{"queue": "payments.*", "role": "submitter"}]},
	    {"name": "operator",  "token": %q, "grants": [{"queue": "payments.*", "role": "operator"}]}
	  ]
	}`, tokSubmitter, tokOperator)
	path := filepath.Join(t.TempDir(), "tokens.json")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
	return path
}

func strPtr(s string) *string   { return &s }
func intPtr(i int) *int         { return &i }
func f64Ptr(f float64) *float64 { return &f }
func durPtr(d time.Duration) *coreconfig.Duration {
	cd := coreconfig.Duration(d)
	return &cd
}

// ---------------------------------------------------------------------------
// Postgres testcontainer (one per test; skips when Docker is unreachable)
// ---------------------------------------------------------------------------

// startIntegrationPostgres brings up a single throwaway Postgres and returns its
// DSN. Like the storage/postgres suite it honors RDQ_TEST_PG_DSN (point at an
// existing disposable database) and RDQ_SKIP_DOCKER, and skips rather than fails
// when no container runtime is reachable so `go test ./...` stays green off-CI.
func startIntegrationPostgres(ctx context.Context, t *testing.T) string {
	t.Helper()
	if dsn := os.Getenv("RDQ_TEST_PG_DSN"); dsn != "" {
		return dsn
	}
	if os.Getenv("RDQ_SKIP_DOCKER") != "" {
		t.Skip("RDQ_SKIP_DOCKER set; skipping testcontainers Postgres integration test")
	}
	ctr, err := pgmod.Run(ctx, "postgres:16-alpine",
		pgmod.WithDatabase("rdq"),
		pgmod.WithUsername("rdq"),
		pgmod.WithPassword("rdq"),
		pgmod.BasicWaitStrategies(),
	)
	if err != nil {
		if isDockerUnavailable(err) {
			t.Skipf("Docker unavailable, skipping Postgres integration test: %v", err)
		}
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = testcontainers.TerminateContainer(ctr, testcontainers.StopContext(stopCtx))
	})
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	return dsn
}

// isDockerUnavailable reports whether err looks like "no container runtime
// reachable" rather than a real provisioning failure — mirrors the classifier in
// the storage/postgres suite so both skip identically off-CI.
func isDockerUnavailable(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, needle := range []string{
		"Cannot connect to the Docker daemon",
		"docker daemon",
		"failed to find a viable Docker",
		"failed to create Docker provider",
		"rootless Docker not found",
		"no such file or directory",
	} {
		if bytes.Contains([]byte(msg), []byte(needle)) {
			return true
		}
	}
	return false
}
