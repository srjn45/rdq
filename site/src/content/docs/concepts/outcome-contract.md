---
title: The outcome contract
description: How every handler result — across Go, Java, and server callbacks — resolves to one canonical outcome that drives retry or dead-letter.
---

Not every language or API signals failure the same way. Go returns an `error`;
Java throws; some functions return a boolean or a status object; some HTTP APIs
return `200` with an error payload. rdq resolves this by defining **one canonical
outcome** and having every host map its local idiom onto it. This is the outcome
contract (PRD §8.2, FR-26..29).

## The canonical outcome

Every handler invocation resolves to exactly one of:

| Outcome | Meaning |
|---|---|
| `SUCCESS` | The call succeeded. Mark the task `SUCCEEDED`. |
| `RETRYABLE_FAILURE(error)` | The call failed transiently. Reschedule with backoff until `max_attempts`. |
| `PERMANENT_FAILURE(error)` | The call failed unrecoverably. **Bypass remaining attempts and dead-letter immediately.** |

The `error` carries `error_type`, `error_message`, and optional detail/stack.
These become the attempt record in the task's failure history (see
[Tasks & the lifecycle](/rdq/concepts/task-lifecycle/)).
`PERMANENT_FAILURE` generalizes the older notion of "non-retryable errors": it is
the escape hatch that says "retrying this cannot help — send it straight to the
DLQ." A fourth outcome, `LEASE_EXPIRED`, exists but is produced by the engine on
lease lapse, not by a handler.

## Default classification (FR-27)

Absent a custom mapper, the universal rule is simple:

> **An error or exception is a failure; any return value — including void/nil — is a success.**

### Go

Handlers are `func(ctx, task) error`:

```go
reg.Register("charge-payment", func(ctx context.Context, task envelope.Envelope) error {
    if err := charge(ctx, task.Payload); err != nil {
        return err // failure — classified below
    }
    return nil // SUCCESS
})
```

- `nil` → `SUCCESS`.
- non-`nil` → a failure, classified retryable-vs-permanent through a precedence
  ladder:
  1. per-call wrappers `rdq.Permanent(err)` / `rdq.Retryable(err)` force the
     decision for one call and override everything below;
  2. **code classifiers** using `errors.Is` / `errors.As` against configured
     sentinel errors or error types;
  3. **config globs** matched against the reported `error_type`;
  4. **default → retryable.**

```go
// Force a decision for a single call:
return rdq.Permanent(fmt.Errorf("card declined: %w", err)) // straight to DLQ
return rdq.Retryable(err)                                   // keep retrying

// Or classify by type/sentinel in queue config (code classifier):
//   errors.Is(err, context.DeadlineExceeded)      → retryable
//   errors.As(err, new(*net.OpError))             → retryable
//   errors.As(err, new(*json.SyntaxError))        → permanent
```

The `error_type` recorded is the wrapper/classifier-supplied name when present,
otherwise the `%T` of the innermost unwrapped error — so a `fmt.Errorf("…: %w",
cause)` still reports the underlying cause's concrete type.

### Java

- normal return → `SUCCESS`;
- a thrown exception → a failure, **retryable by default**, classified against
  the queue's `retryable_errors` / `non_retryable_errors` exception lists. A
  match in the non-retryable (permanent) list wins — an explicit "never retry
  this" bounds a poison pill and must not be silently undone by an also-matching
  retryable rule.

```java
@Override public void handle(Envelope task) throws Exception {
    charge(task.payload());        // returns normally  → SUCCESS
    // throw new CardDeclinedException(...);  // listed non-retryable → DLQ
    // throw new SocketTimeoutException(...);  // default/listed retryable → retry
}
```

## Custom result mappers (FR-28)

Many functions signal failure through a **return value** — a `false`, a status
enum, a `{"status":"FAILED"}` response object — not by throwing. For these,
provide a per-queue **`OutcomeMapper`**: `(returnValue, error) → Outcome`.

When provided, the mapper is **authoritative**: it fully replaces the FR-27
default and sits at the top of the classification ladder. It sees both the return
value and any error/exception, so it can also reclassify thrown errors — for
example, treat a specific "already processed" exception as `SUCCESS` for
idempotent replays. Crucially, the mapper also **supplies the error description**
recorded in the attempt history, so a `false` or `{"status":"FAILED"}` return
still yields a meaningful DLQ entry rather than an empty one.

```go
// A handler that returns a status object instead of an error:
mapper := func(ret any, err error) rdq.Outcome {
    if err != nil {
        return rdq.RetryableFailure(err)
    }
    switch ret.(*Response).Status {
    case "OK":       return rdq.Success()
    case "THROTTLED": return rdq.RetryableFailure(errors.New("downstream throttled"))
    default:          return rdq.PermanentFailure(fmt.Errorf("rejected: %s", ret.(*Response).Status))
    }
}
```

## Callback response mapping — server mode (FR-29)

In [`rdq-server`](/rdq/guides/rdq-server/), a handler is a remote **callback**
over HTTP or gRPC. The engine maps the callback's response — or a timeout — onto
the same canonical outcome. Defaults:

### HTTP

| Response | Outcome |
|---|---|
| `2xx` | `SUCCESS` |
| `408`, `429`, `5xx` | `RETRYABLE_FAILURE` |
| other `4xx` | `PERMANENT_FAILURE` |
| timeout / connection failure | `RETRYABLE_FAILURE` |

### gRPC

| Status code | Outcome |
|---|---|
| `OK` | `SUCCESS` |
| `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `ABORTED` | `RETRYABLE_FAILURE` |
| other non-OK | `PERMANENT_FAILURE` |

These defaults are **overridable per queue**, including an optional
**response-body mapper** for APIs that signal failure inside a `200` — e.g.
inspect a JSON `status` field and treat `{"status":"FAILED"}` as a permanent
failure. This is the callback-mode analogue of the SDK `OutcomeMapper`.

## Why this matters

Because outcomes are canonical, the *entire* retry and DLQ machinery is
identical whether work runs as an in-process Go function, a Java handler, or a
remote HTTP callback. A callback that times out is just another
`RETRYABLE_FAILURE`; a `4xx` is just a `PERMANENT_FAILURE`. One engine, one
contract, every host.

## See also

- [Tasks, attempts & the lifecycle](/rdq/concepts/task-lifecycle/)
- [The wire envelope](/rdq/concepts/wire-envelope/)
- [Queue configuration & retry policies](/rdq/guides/queue-configuration/)
- [Go SDK](/rdq/guides/go-sdk/) · [Java SDK](/rdq/guides/java-sdk/)
- [Running rdq-server](/rdq/guides/rdq-server/)
