# rdq Go SDK

Go client for the **rdq** durable task queue (design docs `docs/design/`).  
Module: `github.com/srjn45/rdq/sdk-go`

---

## Install

```bash
go get github.com/srjn45/rdq/sdk-go
```

For Postgres-backed workers, also add the storage binding:

```bash
go get github.com/srjn45/rdq/storage/postgres
```

---

## Quick start

### Submit a task (client-only, no engine dependency)

```go
import (
    "github.com/srjn45/rdq/sdk-go/submit"
)

env, err := submit.Submit("payments.charge", "payments.handler", payload,
    submit.WithIdempotencyKey("order-42"),       // safe to retry on timeout
    submit.WithHeader("trace-id", traceID),
)
if err != nil {
    return err
}
// hand env to your storage backend
if err := store.Enqueue(ctx, *env); err != nil {
    return err
}
```

`submit.Submit` never touches storage — it builds the task envelope and assigns
an id. Re-submitting with the same idempotency key always produces the same id,
so storage dedupes on conflict.

---

### Consume tasks (worker)

```go
import (
    "context"
    "errors"
    "time"

    "github.com/srjn45/rdq/core/envelope"
    "github.com/srjn45/rdq/core/policy"
    rdq "github.com/srjn45/rdq/sdk-go"
)

// 1. Register a handler for the handler_ref used at submit time.
rdq.Register("payments.handler", func(ctx context.Context, task envelope.Envelope) error {
    // return nil → SUCCEEDED
    // return rdq.Retryable(err) → schedule a retry
    // return rdq.Permanent(err) → dead-letter immediately, no further retries
    return processPayment(ctx, task.Payload)
})

// 2. Configure the queue (retry budget, backoff, concurrency).
spec := rdq.QueueSpec{
    Queue:          "payments.charge",
    MaxAttempts:    8,
    Backoff:        policy.Backoff{Initial: 2 * time.Second, Multiplier: 2, Max: 10 * time.Minute},
    Classifier:     policy.Classifier{},
    Lease:          60 * time.Second,
    HandlerTimeout: 55 * time.Second,
    BatchSize:      16,
    Concurrency:    4,
    PollInterval:   500 * time.Millisecond,
}

// 3. Bind to a storage backend and run.
w, err := rdq.NewWorker(store, []rdq.QueueSpec{spec})
if err != nil {
    return err
}
return w.Run(ctx) // blocks until ctx is cancelled
```

---

### Outcome classification

```go
// Permanent — dead-letter on first failure, skip remaining attempts.
return rdq.Permanent(errors.New("card declined: do not retry"))

// Retryable — force another attempt even if the code classifier would mark it permanent.
return rdq.Retryable(fmt.Errorf("rate limited: %w", err))

// OutcomeMapper — top-of-ladder hook; return ok=true to short-circuit all lower layers.
var mapper rdq.OutcomeMapper = func(err error) (rdq.Decision, bool) {
    var rateLimited *RateLimitError
    if errors.As(err, &rateLimited) {
        return rdq.DecisionRetryable, true
    }
    return 0, false // defer to lower layers
}

spec := rdq.QueueSpec{
    Classifier: policy.Classifier{Mapper: mapper},
    // ...
}
```

---

### Sync-retry (in-process fast path)

```go
import rdq "github.com/srjn45/rdq/sdk-go"

retrier := rdq.NewSyncRetrier(nil) // or build from config: NewSyncRetrier(cfg.SyncRetry)
err := retrier.Run(ctx,
    func() error { return callDownstream(ctx) },    // attempted inline
    func() error { return store.Enqueue(ctx, *env) }, // fallback: durable enqueue
)
```

Use sync-retry when the operation typically succeeds on the first try and you
want to avoid the latency of a full enqueue/claim cycle for the happy path.

---

### Config via builder

```go
import rdq "github.com/srjn45/rdq/sdk-go"

_, cfg, err := rdq.Queue("payments.charge").
    MaxAttempts(8).
    InitialBackoff(2 * time.Second).
    BackoffMultiplier(2.0).
    MaxBackoff(10 * time.Minute).
    Jitter(0.2).
    Lease(60 * time.Second).
    SyncRetryAttempts(2).
    SyncRetryBackoff(100 * time.Millisecond).
    Build()
```

The builder produces a `*config.QueueConfig` — the same struct `LoadYAML`
produces — suitable for server-side config or serialisation. Use `LoadYAML` for
the YAML-file alternative (see next section).

---

### Config via YAML

```go
import rdq "github.com/srjn45/rdq/sdk-go"

yaml := []byte(`
config_version: 1
defaults:
  retry:
    max_attempts: 5
    initial_backoff: 1s
    backoff_multiplier: 2.0
    max_backoff: 10m
    jitter: 0.1
queues:
  payments.charge:
    retry:
      max_attempts: 8
    sync_retry:
      attempts: 2
      backoff: 100ms
`)

cfg, err := rdq.LoadYAML(yaml)
if err != nil { ... }

resolved, err := cfg.Resolved("payments.charge") // inherits defaults
```

The builder and `LoadYAML` are structurally equivalent: same input produces the
same `QueueConfig`. Pick one per process — don't mix both for the same queue.

---

## Run the example

The `examples/consumer` directory contains a runnable demo that submits one
task, watches it fail on every attempt, and then prints the DLQ entry with its
full attempt history.

**Prerequisites:** a Postgres instance (or let Docker provide one via
testcontainers when running the test).

```bash
# Option A: bring your own Postgres
cd sdk-go/examples/consumer
RDQ_POSTGRES_DSN=postgres://rdq:rdq@localhost:5432/rdq go run .

# Option B: run the self-contained e2e test (requires Docker)
cd sdk-go
go test ./examples/consumer/...

# Skip the Docker test in CI without a container runtime
RDQ_SKIP_DOCKER=1 go test ./...
```

Expected output:

```
submitted task 01JZ...  to queue "example.demo"
  handler called: attempt #1 for task 01JZ... — returning retryable error
  handler called: attempt #2 for task 01JZ... — returning retryable error
  handler called: attempt #3 for task 01JZ... — returning retryable error

── result ───────────────────────────────────────────────
  id           : 01JZ...
  queue        : example.demo
  status       : DEAD
  attempt_count: 3
  attempts     :
    #1   outcome=RETRYABLE_FAILURE       error="transient: service unavailable"
    #2   outcome=RETRYABLE_FAILURE       error="transient: service unavailable"
    #3   outcome=RETRYABLE_FAILURE       error="transient: service unavailable"
```

---

## API surface

| Symbol | Package | Purpose |
|---|---|---|
| `Register` | `sdk-go` | Bind a `HandlerFunc` to a handler_ref |
| `NewWorker` / `Worker.Run` | `sdk-go` | Drive the claim-process-outcome loop |
| `QueueSpec` | `sdk-go` | Runtime per-queue config (retry, backoff, concurrency) |
| `Permanent` / `Retryable` | `sdk-go` | Force a classification decision from inside a handler |
| `OutcomeMapper` | `sdk-go` | Top-of-ladder classification hook |
| `Queue(name)…Build()` | `sdk-go` | Code builder for `config.QueueConfig` |
| `LoadYAML` | `sdk-go` | YAML loader for `config.QueueConfig` |
| `NewSyncRetrier` / `SyncRetrier.Run` | `sdk-go` | In-process fast-path retries |
| `submit.Submit` | `sdk-go/submit` | Build an envelope without touching storage |

For full godoc: `go doc github.com/srjn45/rdq/sdk-go`.
