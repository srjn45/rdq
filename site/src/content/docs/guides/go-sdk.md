---
title: Go SDK
description: Embed rdq in a Go service — open storage, register handlers, submit tasks, and run a durable retry worker.
---

The Go SDK embeds the rdq engine directly in your process. There is no extra
infrastructure to run: handlers are ordinary Go functions, and retry state lives
in the storage backend you already operate (PostgreSQL in v1). This guide walks
from install to a running worker, then covers the outcome contract and graceful
shutdown.

Module path: `github.com/srjn45/rdq/sdk-go`.

## Install

```bash
go get github.com/srjn45/rdq/sdk-go
```

For Postgres-backed workers, also add the storage binding:

```bash
go get github.com/srjn45/rdq/storage/postgres
```

The engine ships as two importable surfaces:

| Import | Use it when |
|---|---|
| `github.com/srjn45/rdq/sdk-go/submit` | You only need to submit failed work — no worker, no engine dependency. |
| `github.com/srjn45/rdq/sdk-go` | You run a worker that claims and executes tasks. |

This is the "submit here, execute there" split: a producer service depends only
on `submit`, while a separate consumer service pulls in the full engine.

## Open storage and migrate

Apply the schema once — the binding refuses to open against an unmigrated
database — then open a store:

```go
import "github.com/srjn45/rdq/storage/postgres"

db, err := postgres.Open("postgres://rdq:rdq@localhost:5432/rdq?sslmode=disable")
if err != nil {
    return err
}
if err := postgres.Migrate(ctx, db); err != nil { // idempotent; safe on every boot
    return err
}
store := postgres.New(db)
```

You can also apply migrations out-of-band with `rdq --dsn DSN migrate` (see
[the rdq CLI](/rdq/guides/cli/)).

## Register handlers

A handler is bound to a stable `handler_ref` string — the contract between a
stored task and the code that runs it. Names survive deploys and restarts; never
serialize closures.

```go
import (
    "context"
    "github.com/srjn45/rdq/core/envelope"
    rdq "github.com/srjn45/rdq/sdk-go"
)

rdq.Register("payments.handler", func(ctx context.Context, task envelope.Envelope) error {
    // decode task.Payload, do real work …
    // return nil → SUCCEEDED
    // return err → retried per the queue policy
    return processPayment(ctx, task.Payload)
})
```

Handlers must be **idempotent**: rdq is at-least-once, so a task may be delivered
more than once (for example after a lease expires mid-execution). See
[the task lifecycle](/rdq/concepts/task-lifecycle/).

## Submit a task

From the producer side, build an envelope with the `submit` sub-package and hand
it to storage. `submit.Submit` never touches storage itself — it constructs the
task envelope and assigns an id:

```go
import "github.com/srjn45/rdq/sdk-go/submit"

env, err := submit.Submit("payments.charge", "payments.handler", payload,
    submit.WithIdempotencyKey("order-42"), // same key → same id, safe to retry on timeout
    submit.WithHeader("trace-id", traceID),
)
if err != nil {
    return err
}
if err := store.Enqueue(ctx, *env); err != nil {
    return err // submission failure surfaces — rdq never silently drops work
}
```

`Submit(queue, handler_ref, payload, opts…)` binds the task to a queue and the
handler that will execute it. Re-submitting with the same idempotency key always
produces the same id, so storage dedupes on conflict.

## Run a worker

Configure each queue with a `QueueSpec` and run the worker. `Run` blocks until
the context is cancelled.

```go
import (
    "time"
    "github.com/srjn45/rdq/core/policy"
    rdq "github.com/srjn45/rdq/sdk-go"
)

spec := rdq.QueueSpec{
    Queue:          "payments.charge",
    MaxAttempts:    8,
    Backoff:        policy.Backoff{Initial: 2 * time.Second, Multiplier: 2, Max: 10 * time.Minute},
    Classifier:     policy.Classifier{},
    Lease:          60 * time.Second,
    HandlerTimeout: 55 * time.Second, // must be <= Lease
    BatchSize:      16,
    Concurrency:    4,
    PollInterval:   500 * time.Millisecond,
}

w, err := rdq.NewWorker(store, []rdq.QueueSpec{spec})
if err != nil {
    return err
}
return w.Run(ctx) // blocks until ctx is cancelled
```

A `Backoff` with `Multiplier: 2` is exponential; `Multiplier: 1` is linear. The
engine adds jitter and caps each delay at `Max`. See
[queue configuration](/rdq/guides/queue-configuration/) for the full policy
surface and the [`Queue(...).Build()` builder](/rdq/reference/configuration/) if
you prefer config objects or YAML.

Run N instances against one database to scale horizontally — atomic claims and
leases guarantee no two workers ever run the same task.

## The outcome contract in Go

Every invocation resolves to exactly one canonical outcome. The default rule is
simple: **`nil` is success; any non-nil error is a failure**, retryable by
default and classified against the queue's error lists via `errors.Is`/`errors.As`.

Force a decision from inside the handler with the wrappers:

```go
import "errors"

// Permanent — dead-letter on first failure, skip remaining attempts.
return rdq.Permanent(errors.New("card declined: do not retry"))

// Retryable — force another attempt even if a classifier would mark it permanent.
return rdq.Retryable(fmt.Errorf("rate limited: %w", err))
```

For return-value idioms (booleans, status structs) or hierarchy-aware rules,
install an `OutcomeMapper` — the top-of-ladder hook. Return `ok=true` to
short-circuit every lower layer:

```go
var mapper rdq.OutcomeMapper = func(err error) (rdq.Decision, bool) {
    var rateLimited *RateLimitError
    if errors.As(err, &rateLimited) {
        return rdq.DecisionRetryable, true
    }
    return 0, false // defer to lower layers
}

spec := rdq.QueueSpec{
    Classifier: policy.Classifier{Mapper: mapper},
    // …
}
```

Classification precedence, from most specific: `OutcomeMapper` → `Permanent`/`Retryable`
wrappers → code classifiers (`errors.Is/As`) → config globs → default (retryable).
Full details in [the outcome contract](/rdq/concepts/outcome-contract/).

## Sync-retry fast path

When an operation usually succeeds on the first try, you can attempt a few
in-process retries before paying for a durable enqueue/claim cycle:

```go
retrier := rdq.NewSyncRetrier(nil) // or NewSyncRetrier(cfg.SyncRetry)
err := retrier.Run(ctx,
    func() error { return callDownstream(ctx) },      // attempted inline
    func() error { return store.Enqueue(ctx, *env) }, // fallback: durable enqueue
)
```

## Graceful shutdown

`Worker.Run` stops on context cancellation. Wire it to `SIGTERM` so a rollout
drains cleanly instead of abandoning in-flight tasks to lease expiry:

```go
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
defer stop()

if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
    log.Fatal(err)
}
```

On cancellation the worker stops claiming new tasks and lets in-flight handlers
finish within their lease before `Run` returns.

## See also

- [The outcome contract](/rdq/concepts/outcome-contract/)
- [Tasks, attempts & the lifecycle](/rdq/concepts/task-lifecycle/)
- [Queue configuration & retry policies](/rdq/guides/queue-configuration/)
- [Configuration reference](/rdq/reference/configuration/)
- [Storage backends & sizing](/rdq/reference/storage-backends/)
