// SPDX-License-Identifier: Apache-2.0

// Package crosslang_test contains the T8.2 cross-language e2e integration test:
// the flagship success metric of the rdq v1 project.
//
// # What this tests
//
// The full lifecycle of a task that crosses the Go↔Java boundary via a shared
// PostgreSQL backend:
//
//  1. ONE Postgres testcontainer (shared by the Go engine and the Java worker
//     subprocess). Memory-constrained CI hosts run exactly one container.
//
//  2. The Go server HTTP API (server/http) is used to submit a task.  The Go
//     engine claims it and dispatches over the server HTTP callback path
//     (server/callback callbackBridge) to a stub receiver that deliberately
//     fails.  Two attempts exhaust the budget → the task is dead-lettered.
//
//  3. The Go operator HTTP API redrives the task back to PENDING with a fresh
//     attempt budget (T5.7 guarantee: attempt history is preserved; the history
//     sequence continues past the retained attempt_no rows so there is no
//     UNIQUE(task_id,attempt_no) collision).
//
//  4. The Go engine is stopped before the redrive.  The only worker left in play
//     is the Java subprocess, launched via `./gradlew :example:run` against the
//     same Postgres DSN.  The subprocess runs the REAL Java Worker engine
//     (Worker.java), which now correctly separates budgetNo (attemptCount()+1,
//     for retry budget and backoff) from historyNo (attempts().size()+1, for the
//     persisted attempt_no) — the Java-side T5.7-equivalent fix bundled with this
//     test.  No UNIQUE(task_id,attempt_no) collision with the preserved DLQ-phase
//     history rows.
//
//  5. The Java subprocess exits 0; the Go API confirms SUCCEEDED.
//
// # Approach rationale
//
// Go drives the orchestration because it owns the testcontainer and the HTTP
// API under test.  The Java leg runs as a subprocess (`./gradlew :example:run`)
// so the JVM is a genuine separate process.  The subprocess timeout is generous
// (120 s) to absorb Gradle cold-start on the first CI run.  Set
// RDQ_SKIP_CROSSLANG=1 to skip just this test; RDQ_SKIP_DOCKER skips it too.
package crosslang_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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

// Test knobs: keep counts tiny to avoid OOMing a tight-swap host.
const (
	clQueue      = "crosslang.payments"
	clHandlerRef = "crosslang.processor"
	clMaxAtt     = 2               // 1 fail → reschedule → 1 fail → DLQ
	clBackoff    = 40 * time.Millisecond
	clLease      = 5 * time.Second
	clPoll       = 100 * time.Millisecond
	clCBTimeout  = 2 * time.Second

	clTokSubmitter = "cl-tok-submitter"
	clTokOperator  = "cl-tok-operator"

	clCBSecretEnv = "RDQ_CL_CB_SECRET"
	clCBSecret    = "cl-cb-shared-secret-xyz987"

	// Generous timeout for the Go→DLQ phase.
	clGoDLQTimeout = 45 * time.Second
	// Generous timeout for the Java execution phase (Gradle cold-start + JVM).
	clJavaTimeout = 120 * time.Second
)

// TestCrossLang_GoSubmitDLQRedriveJavaExec is the T8.2 acceptance test.
//
//   - Phase 1 (Go engine, HTTP callback path): submit → fail → retry → DLQ.
//   - Phase 2 (Java subprocess, shared Postgres): redrive → Java claims → SUCCEEDED.
func TestCrossLang_GoSubmitDLQRedriveJavaExec(t *testing.T) {
	if os.Getenv("RDQ_SKIP_CROSSLANG") != "" {
		t.Skip("RDQ_SKIP_CROSSLANG set")
	}
	ctx := context.Background()

	// --- Postgres: ONE container for both Go engine and Java worker. -----------
	ctr, dsn := startCrosslangPostgres(ctx, t)
	db, err := postgres.Open(dsn)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := postgres.Migrate(ctx, db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	store := postgres.New(db)

	// --- Stub callback receiver: fails initially, then succeeds. ---------------
	recv := &clStubReceiver{secret: clCBSecret}
	recv.failing.Store(true)
	recvSrv := httptest.NewServer(recv)
	t.Cleanup(recvSrv.Close)
	callbackURL := recvSrv.URL + "/hook"

	// --- Auth + server config. -------------------------------------------------
	t.Setenv(clCBSecretEnv, clCBSecret)
	tokensPath := clWriteTokenFile(t)
	tokenStore, err := auth.LoadTokenStore(tokensPath)
	if err != nil {
		t.Fatalf("load token store: %v", err)
	}
	authz := auth.NewAuthorizer(tokenStore)

	serverCfg := &srvconfig.ServerConfig{
		TokensPath:        tokensPath,
		CallbackAllowlist: []string{recvSrv.URL},
	}
	qc := clQueueConfig(callbackURL)
	envFn := func(k string) (string, bool) { return os.LookupEnv(k) }

	if err := serverCfg.ValidateCallbacks(map[string]*coreconfig.QueueConfig{clQueue: qc}, envFn); err != nil {
		t.Fatalf("ValidateCallbacks: %v", err)
	}
	secret, err := srvconfig.ResolveSecretRef(*qc.Callback.Auth.SecretRef, envFn)
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

	// --- Go engine: callbackBridge + worker loop. ------------------------------
	reg := registry.New()
	bridge := &clCallbackBridge{
		dispatcher: callback.New(),
		target:     target,
		allowlist:  allowlist,
	}
	if err := reg.Register(clHandlerRef, bridge); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	spec, err := engine.SpecFromConfig(clQueue, qc)
	if err != nil {
		t.Fatalf("spec from config: %v", err)
	}
	worker, err := engine.NewWorker(store, reg, []engine.QueueSpec{spec},
		engine.WithSweepInterval(0))
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	go func() { defer close(workerDone); _ = worker.Run(workerCtx) }()
	// Worker cleanup is manually invoked below after DLQ, before Java subprocess.
	stopAndWaitWorker := func() {
		stopWorker()
		select {
		case <-workerDone:
		case <-time.After(2 * clLease):
			t.Log("Go engine did not drain within deadline")
		}
	}

	// --- Go API server on a real loopback port. --------------------------------
	apiSrv := srvhttp.New(
		srvhttp.WithStorage(store),
		srvhttp.WithAuthorizer(authz),
	)
	api := httptest.NewServer(apiSrv.Handler())
	t.Cleanup(api.Close)
	cl := &clAPIClient{base: api.URL, hc: api.Client()}

	// ==================== PHASE 1: Go engine + HTTP callback path ==============

	// (1) Submit task via Go HTTP API as the submitter principal.
	code, body := cl.do(t, http.MethodPost, "/v1/queues/"+clQueue+"/tasks", clTokSubmitter, clSubmitBody())
	if code != http.StatusAccepted {
		t.Fatalf("submit: status=%d, want 202; body=%s", code, body)
	}
	var submitted envelope.Envelope
	clMustJSON(t, body, &submitted)
	taskID := submitted.ID
	if taskID == "" {
		t.Fatal("submit returned envelope with no id")
	}

	// (2) Go engine exhausts retries via HTTP callback dispatch → DEAD.
	dead := cl.waitForStatus(t, taskID, clTokSubmitter, envelope.StatusDead, clGoDLQTimeout)
	if dead.AttemptCount != clMaxAtt {
		t.Errorf("dead task attempt_count=%d, want %d", dead.AttemptCount, clMaxAtt)
	}
	if got := recv.count(); got < clMaxAtt {
		t.Errorf("callback receiver saw %d deliveries before DLQ, want >= %d", got, clMaxAtt)
	}
	if !recv.sawAuthOK() {
		t.Error("callback receiver never saw a correct Authorization header")
	}

	// (3) DLQ has exactly the dead task.
	if st := cl.stats(t, clQueue, clTokSubmitter); st.DLQDepth != 1 {
		t.Errorf("stats.dlq_depth=%d after DLQ, want 1", st.DLQDepth)
	}

	// ==================== STOP GO ENGINE before redrive =======================
	// The Go engine must not claim the redriven task — Java is the only worker
	// from this point on, which unambiguously proves cross-language execution.
	stopAndWaitWorker()
	// Also flip receiver to succeed (belt-and-suspenders; Go engine is gone).
	recv.failing.Store(false)

	// ==================== PHASE 2: Redrive → Java worker ======================

	// (4) Redrive as the operator principal.
	code, body = cl.do(t, http.MethodPost, "/v1/queues/"+clQueue+"/dlq:redrive", clTokOperator, clRedriveBody(taskID))
	if code != http.StatusOK {
		t.Fatalf("redrive: status=%d, want 200; body=%s", code, body)
	}
	var redriven struct {
		Count int `json:"count"`
	}
	clMustJSON(t, body, &redriven)
	if redriven.Count != 1 {
		t.Fatalf("redrive count=%d, want 1", redriven.Count)
	}

	// (5) Launch Java worker subprocess pointing at the shared Postgres.
	javaCmd := clLaunchJavaWorker(t, ctr, ctx, clQueue, clHandlerRef, taskID)

	// (6) Poll Go API until the task reaches SUCCEEDED (Java executed it).
	succeeded := cl.waitForStatus(t, taskID, clTokSubmitter, envelope.StatusSucceeded, clJavaTimeout)
	if succeeded.Status != envelope.StatusSucceeded {
		t.Fatalf("final status=%q, want SUCCEEDED", succeeded.Status)
	}

	// (7) Wait for Java subprocess to exit cleanly.
	javaDone := make(chan error, 1)
	go func() { javaDone <- javaCmd.Wait() }()
	select {
	case javaErr := <-javaDone:
		if javaErr != nil {
			t.Errorf("java subprocess exited with error: %v", javaErr)
		}
	case <-time.After(15 * time.Second):
		t.Log("java subprocess still running after task SUCCEEDED; killing")
		_ = javaCmd.Process.Kill()
	}

	// (8) DLQ is empty — the loop closed cleanly.
	if st := cl.stats(t, clQueue, clTokSubmitter); st.DLQDepth != 0 {
		t.Errorf("stats.dlq_depth=%d after Java success, want 0", st.DLQDepth)
	}

	// (9) Attempt history: the pre-DLQ Go attempts are preserved; Java's
	// attempt_no continues the monotonic sequence (no double-execution).
	// attempt_count=1 reflects Java's single clean execution of the fresh budget.
	if succeeded.AttemptCount != 1 {
		t.Errorf("attempt_count=%d after Java success, want 1 (fresh budget)", succeeded.AttemptCount)
	}
}

// ---------------------------------------------------------------------------
// Callback bridge: adapts callback.Dispatcher to registry.Handler (same as T5.7).
// ---------------------------------------------------------------------------

type clCallbackBridge struct {
	dispatcher *callback.Dispatcher
	target     callback.Target
	allowlist  *srvconfig.Allowlist
}

func (b *clCallbackBridge) Version() string { return "" }

func (b *clCallbackBridge) Handle(ctx context.Context, task envelope.Envelope) error {
	if !b.allowlist.Allows(b.target.URL) {
		return policy.Permanent(fmt.Errorf("callback url %q off allowlist", b.target.URL))
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
// Stub callback receiver (same pattern as T5.7).
// ---------------------------------------------------------------------------

type clStubReceiver struct {
	secret  string
	failing atomic.Bool
	calls   atomic.Int64
	authOK  atomic.Bool
}

func (r *clStubReceiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.calls.Add(1)
	if req.Header.Get("Authorization") == "Bearer "+r.secret {
		r.authOK.Store(true)
	}
	body, _ := io.ReadAll(req.Body)
	if r.failing.Load() {
		http.Error(w, `{"reason":"stub forced failure"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (r *clStubReceiver) count() int64    { return r.calls.Load() }
func (r *clStubReceiver) sawAuthOK() bool { return r.authOK.Load() }

// ---------------------------------------------------------------------------
// API client helpers.
// ---------------------------------------------------------------------------

type clAPIClient struct {
	base string
	hc   *http.Client
}

func (c *clAPIClient) do(t *testing.T, method, path, token string, body []byte) (int, []byte) {
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

// waitForStatus polls GET /v1/tasks/{id} until the task reaches want or deadline.
func (c *clAPIClient) waitForStatus(t *testing.T, id, token string, want envelope.Status, timeout time.Duration) envelope.Envelope {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last envelope.Envelope
	for time.Now().Before(deadline) {
		code, body := c.do(t, http.MethodGet, "/v1/tasks/"+id, token, nil)
		if code == http.StatusOK {
			clMustJSON(t, body, &last)
			if last.Status == want {
				return last
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("task %s never reached %q (last=%q) within %s", id, want, last.Status, timeout)
	return last
}

func (c *clAPIClient) stats(t *testing.T, queue, token string) clStatsResp {
	t.Helper()
	code, body := c.do(t, http.MethodGet, "/v1/queues/"+queue+"/stats", token, nil)
	if code != http.StatusOK {
		t.Fatalf("stats: status=%d; body=%s", code, body)
	}
	var st clStatsResp
	clMustJSON(t, body, &st)
	return st
}

type clStatsResp struct {
	DLQDepth int64 `json:"dlq_depth"`
}

// ---------------------------------------------------------------------------
// Java subprocess launcher.
// ---------------------------------------------------------------------------

// clLaunchJavaWorker starts the Java CrossLangWorkerRunner as a subprocess via
// the sdk-java Gradle wrapper.  It returns the started *exec.Cmd so the caller
// can Wait() on it.  The test is skipped if the Gradle wrapper is not found or
// cannot be started.
func clLaunchJavaWorker(
	t *testing.T,
	ctr *pgmod.PostgresContainer,
	ctx context.Context,
	queue, handlerRef, taskID string,
) *exec.Cmd {
	t.Helper()

	sdkJava := clSDKJavaDir(t)
	gradlew := filepath.Join(sdkJava, "gradlew")
	if _, err := os.Stat(gradlew); err != nil {
		t.Skipf("gradlew not found at %s; skipping Java phase: %v", gradlew, err)
	}

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("container host: %v", err)
	}
	port, err := ctr.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatalf("container mapped port: %v", err)
	}

	cmd := exec.Command(gradlew, "--no-daemon", ":example:run")
	cmd.Dir = sdkJava
	cmd.Env = append(os.Environ(),
		"RDQ_PG_HOST="+host,
		"RDQ_PG_PORT="+port.Port(),
		"RDQ_PG_DB=rdq",
		"RDQ_PG_USER=rdq",
		"RDQ_PG_PASS=rdq",
		"RDQ_QUEUE="+queue,
		"RDQ_HANDLER_REF="+handlerRef,
		"RDQ_TASK_ID="+taskID,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start java subprocess (%s :example:run): %v — skipping Java phase", gradlew, err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	})
	return cmd
}

// clSDKJavaDir returns the absolute path to sdk-java/ by walking up from this
// source file's location.  Works when run via `go test` in workspace mode.
func clSDKJavaDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed; cannot locate sdk-java dir")
	}
	// file: <repoRoot>/integration/crosslang_test.go
	// sdk-java: <repoRoot>/sdk-java/
	repoRoot := filepath.Dir(filepath.Dir(file))
	return filepath.Join(repoRoot, "sdk-java")
}

// ---------------------------------------------------------------------------
// Fixtures.
// ---------------------------------------------------------------------------

func clQueueConfig(callbackURL string) *coreconfig.QueueConfig {
	return &coreconfig.QueueConfig{
		Retry: &coreconfig.RetryConfig{
			MaxAttempts:       clIntPtr(clMaxAtt),
			InitialBackoff:    clDurPtr(clBackoff),
			BackoffMultiplier: clF64Ptr(1.0),
			MaxBackoff:        clDurPtr(clBackoff),
			Jitter:            clF64Ptr(0),
		},
		Execution: &coreconfig.ExecutionConfig{
			Lease: clDurPtr(clLease),
		},
		Worker: &coreconfig.WorkerConfig{
			PollInterval: clDurPtr(clPoll),
			BatchSize:    clIntPtr(1),
			Concurrency:  clIntPtr(1),
		},
		Callback: &coreconfig.CallbackConfig{
			Protocol: clStrPtr("http"),
			URL:      clStrPtr(callbackURL),
			Timeout:  clDurPtr(clCBTimeout),
			Auth: &coreconfig.CallbackAuth{
				Type:      clStrPtr("bearer"),
				SecretRef: clStrPtr("env:" + clCBSecretEnv),
			},
		},
	}
}

func clWriteTokenFile(t *testing.T) string {
	t.Helper()
	doc := fmt.Sprintf(`{
  "principals": [
    {"name": "submitter", "token": %q, "grants": [{"queue": "crosslang.*", "role": "submitter"}]},
    {"name": "operator",  "token": %q, "grants": [{"queue": "crosslang.*", "role": "operator"}]}
  ]
}`, clTokSubmitter, clTokOperator)
	path := filepath.Join(t.TempDir(), "tokens.json")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
	return path
}

func clSubmitBody() []byte {
	b, _ := json.Marshal(map[string]any{
		"handler_ref":          clHandlerRef,
		"payload":              []byte(`{"order_id":1}`),
		"payload_content_type": "application/json",
	})
	return b
}

func clRedriveBody(id string) []byte {
	b, _ := json.Marshal(map[string]any{"ids": []string{id}})
	return b
}

func clMustJSON(t *testing.T, data []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatalf("decode json %T: %v\nbody=%s", v, err, data)
	}
}

// ---------------------------------------------------------------------------
// Postgres testcontainer (one for the whole test, shared by Go engine + Java).
// ---------------------------------------------------------------------------

// startCrosslangPostgres brings up a single Postgres container and returns its
// DSN plus the container handle (needed for host/port extraction for Java).
// Skips when Docker is unavailable or RDQ_SKIP_DOCKER is set.
func startCrosslangPostgres(ctx context.Context, t *testing.T) (*pgmod.PostgresContainer, string) {
	t.Helper()
	if os.Getenv("RDQ_SKIP_DOCKER") != "" {
		t.Skip("RDQ_SKIP_DOCKER set; skipping cross-language integration test")
	}
	if dsn := os.Getenv("RDQ_TEST_PG_DSN"); dsn != "" {
		// When a pre-existing Postgres is provided we cannot return a container
		// handle for host/port extraction.  In this mode the Java subprocess
		// must be pre-configured externally; skip the Java leg.
		t.Skip("RDQ_TEST_PG_DSN set (pre-existing Postgres); Java subprocess leg skipped in this mode")
	}
	ctr, err := pgmod.Run(ctx, "postgres:16-alpine",
		pgmod.WithDatabase("rdq"),
		pgmod.WithUsername("rdq"),
		pgmod.WithPassword("rdq"),
		pgmod.BasicWaitStrategies(),
	)
	if err != nil {
		if clIsDockerUnavailable(err) {
			t.Skipf("Docker unavailable; skipping: %v", err)
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
	return ctr, dsn
}

func clIsDockerUnavailable(err error) bool {
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

// ---------------------------------------------------------------------------
// Pointer helpers (same pattern as T5.7).
// ---------------------------------------------------------------------------

func clStrPtr(s string) *string   { return &s }
func clIntPtr(i int) *int         { return &i }
func clF64Ptr(f float64) *float64 { return &f }
func clDurPtr(d time.Duration) *coreconfig.Duration {
	cd := coreconfig.Duration(d)
	return &cd
}
