---
title: Observability & metrics
description: rdq's Prometheus metrics, the two flagship DLQ alert signals, structured state-transition logs, and trace-context propagation.
---

rdq exposes what an on-call engineer needs to answer "is the retry engine
healthy, and is anything piling up?" It emits Prometheus metrics labeled by
queue, structured logs on every state transition, and propagates trace context
end to end. This guide covers each, and the two signals you should alert on.

## Prometheus metrics

All instruments live under the `rdq` namespace and are labeled by `queue`
(handler duration adds an `outcome` label). Counters and histograms are pushed by
the engine as events fire; the two gauges are pulled from storage at scrape time.

| Metric | Type | Meaning |
|---|---|---|
| `rdq_task_retries_total` | counter | Retries (reschedule calls) — the retry rate, by queue. |
| `rdq_task_success_after_retry_total` | counter | Tasks that succeeded after ≥ 1 prior failure. |
| `rdq_task_dlq_arrivals_total` | counter | Tasks moved to the DLQ. |
| `rdq_dlq_depth` | gauge | Tasks currently in the dead-letter queue. |
| `rdq_oldest_pending_age_seconds` | gauge | Age of the oldest pending task (0 when empty). |
| `rdq_claim_latency_seconds` | histogram | `ClaimDue` round-trip latency. |
| `rdq_handler_duration_seconds` | histogram | Handler execution time, by `queue` and `outcome` (`success`/`failure`). |

Per-queue label sets are pre-initialized, so a queue appears in the metrics from
the first scrape even before any event fires. The two gauges (`rdq_dlq_depth`,
`rdq_oldest_pending_age_seconds`) are collected by pulling `Stats` from the
storage backend at scrape time, so they reflect the true durable state rather
than an in-process counter that resets on restart.

## The two flagship alerting signals

Alert on **DLQ depth** and **oldest pending task age**. Together they distinguish
the two failure modes that matter:

- **`rdq_dlq_depth` rising** means tasks are giving up — a handler bug or a hard
  downstream failure is dead-lettering work. This is the on-call page: browse the
  DLQ, read the attempt history, ship a fix, then redrive.
- **`rdq_oldest_pending_age_seconds` climbing** means the retry queue is not
  draining — workers are down, under-provisioned, or wedged behind a slow
  downstream. Work is not lost, but it is falling behind.

```yaml
groups:
  - name: rdq
    rules:
      - alert: RdqDlqFilling
        expr: rdq_dlq_depth > 0
        for: 5m
        labels: { severity: page }
        annotations:
          summary: "DLQ for {{ $labels.queue }} has {{ $value }} tasks"

      - alert: RdqRetryQueueStalling
        expr: rdq_oldest_pending_age_seconds > 900
        for: 10m
        labels: { severity: page }
        annotations:
          summary: "Oldest pending task in {{ $labels.queue }} is {{ $value }}s old"
```

Tune the thresholds to each queue's expected backoff schedule — a queue with a
`10m` `max_backoff` legitimately has pending tasks minutes old.

The supporting metrics add context: a high `rdq_task_retries_total` rate with a
healthy `rdq_task_success_after_retry_total` means retries are doing their job; a
rising `rdq_task_dlq_arrivals_total` is the leading indicator behind DLQ depth;
`rdq_claim_latency_seconds` and `rdq_handler_duration_seconds` locate slowness in
storage versus handler code.

## Scraping

In `rdq-server`, the metrics endpoint is exposed for Prometheus to scrape (its
path is set in server config). Embedded SDK workers construct a metrics registry
and serve it on an endpoint of their choosing via the standard
`promhttp.HandlerFor` handler — the registry pre-registers every instrument and
drives the storage-backed gauges.

## Structured logs

rdq writes a structured log line on **every state transition** — submit, claim,
attempt outcome, reschedule, success, and dead-letter — each carrying at least
the **task id** and **queue**, so a single task is traceable across its whole
lifecycle. Payloads are treated as sensitive and are never logged in full.

```json
{"level":"info","event":"attempt.failed","task_id":"01J2ZK7Q...","queue":"payments.charge",
 "attempt_no":3,"outcome":"RETRYABLE_FAILURE","error_type":"TIMEOUT","next_attempt_at":"2026-07-27T14:32:10Z"}
{"level":"warn","event":"task.dead_lettered","task_id":"01J2ZK7Q...","queue":"payments.charge",
 "attempt_count":8,"error_type":"TIMEOUT"}
```

## Trace-context propagation

Trace context propagates through the whole path — **submit → retry → handler
invocation**. A `traceparent` supplied in the task headers at submit time travels
with the task in storage and is restored on every retry, so an attempt made hours
after submission still belongs to the originating trace. In server mode the
`traceparent` header is forwarded on the outbound callback (alongside
`X-RDQ-Task-Id`, `X-RDQ-Queue`, and `X-RDQ-Attempt`), so your receiver's spans
join the same trace. Set the header at submit time:

```go
env, _ := submit.Submit("payments.charge", "payments.handler", payload,
    submit.WithHeader("traceparent", span.SpanContext().TraceParent()),
)
```

## See also

- [The rdq CLI](/rdq/guides/cli/)
- [DLQ analysis & redrive](/rdq/guides/dlq-and-redrive/)
- [Running rdq-server](/rdq/guides/rdq-server/)
- [Tasks, attempts & the lifecycle](/rdq/concepts/task-lifecycle/)
