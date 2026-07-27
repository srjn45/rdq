---
title: "No two workers, one task: how rdq claims work — and the UNIQUE collision redrive taught us"
description: rdq has no coordinator. Correctness comes from atomic claims and leases in the storage backend. Here's the claim/lease model in detail — and a subtle bug where a redriven, lease-expired task collided on a UNIQUE(task_id, attempt_no) constraint, and why the fix was to compute the attempt number from history.
date: 2026-07-27
authors: srjn45
excerpt: rdq spreads work across a fleet of stateless workers with no leader and no membership protocol. The entire correctness argument rests on two primitives — an atomic claim and a lease. This is how they work, and the story of a UNIQUE collision that only appeared when a redriven task's lease expired.
tags:
  - internals
  - postgres
  - concurrency
  - reliability
---

rdq runs a fleet of workers against a shared datastore with **no leader election and no cluster
membership.** There's no coordinator process to ask "whose task is this?" — and that's on
purpose. The entire correctness argument rests on two primitives, both borrowed from the storage
backend: an **atomic claim** and a **lease**. Get those two right and the rest of the system —
"add a node to scale," "a `kill -9` is a non-event" — falls out for free.

This post is the detail behind [the task lifecycle](/rdq/concepts/task-lifecycle/): how a claim
works, why the lease is the whole fault-tolerance story, and a subtle bug that only surfaced when
a **redriven** task's lease expired.

## The one rule

Everything reduces to a single correctness bar:

> **No two workers may ever claim the same task.**

Not "rarely." Not "we detect and reconcile." Never. If two workers can both believe they own a
task, you get concurrent double-execution on top of the at-least-once contract, and no amount of
idempotency guidance saves you from the surprise.

rdq refuses to solve this with a coordinator, because a coordinator is a stateful component with
its own HA story — exactly the thing rdq promises *not* to add. Instead, the claim is delegated to
whatever the storage backend already does atomically.

## The atomic claim

A worker's inner loop is: *find due tasks, take one, run it.* "Find due" is easy — `status =
PENDING AND next_attempt_at <= now()`. The hard part is "take one" without racing every other
worker doing the same query at the same millisecond.

Each storage plugin owns its own atomic-claim mechanics, and this is a hard requirement of the
[storage SPI](/rdq/concepts/storage-spi/):

- **PostgreSQL** — `SELECT … FOR UPDATE SKIP LOCKED`. The row lock makes the claim atomic;
  `SKIP LOCKED` means a worker steps over rows another worker has already locked instead of
  blocking on them. Ten workers hit the same query and walk away with ten *different* tasks.
- **Redis** — an atomic sorted-set pop, scored by `next_attempt_at`, in a Lua script.
- **MongoDB** — `findAndModify`, which claims and updates in one atomic step.

In Postgres the claim looks roughly like this:

```sql
UPDATE tasks
SET status = 'IN_FLIGHT',
    lease_expires_at = now() + $lease
WHERE id IN (
    SELECT id FROM tasks
    WHERE queue = $1 AND status = 'PENDING' AND next_attempt_at <= now()
    ORDER BY next_attempt_at
    FOR UPDATE SKIP LOCKED
    LIMIT $batch
)
RETURNING *;
```

The `SELECT … FOR UPDATE SKIP LOCKED` does the claiming; the surrounding `UPDATE` flips the task
to `IN_FLIGHT` and stamps a lease in the same transaction. There is no window between "I saw it"
and "I own it."

## The lease is the fault-tolerance story

An atomic claim stops two *live* workers from grabbing the same task. But what about a worker that
claims a task and then dies — OOM-killed, crashed, partitioned off the network — while the task
sits `IN_FLIGHT`? Without more, that task is stuck forever, owned by a ghost.

That's what the **lease** is for. Every claim stamps `lease_expires_at`. A worker is expected to
finish (or heartbeat) before the lease runs out. If it doesn't, the task becomes reclaimable:

```sql
-- reclaim: an IN_FLIGHT task whose lease has expired is fair game again
WHERE status = 'IN_FLIGHT' AND lease_expires_at <= now()
```

The reclaiming worker treats the expiry as a failed attempt — the handler exceeded its visibility
timeout — and reschedules per policy. A `kill -9` mid-task doesn't lose work and doesn't hang the
queue; it just costs one lease interval. This is why rdq can say node death is a non-event: the
lease turns a crash into an ordinary retry.

## Where it got subtle: LEASE_EXPIRED meets redrive

Here's the part that only shows up in production.

rdq keeps a full **attempt history** — one row per execution attempt, and that history is what
travels into the DLQ. Attempts are keyed by `UNIQUE(task_id, attempt_no)`: attempt 1, attempt 2,
and so on, one row each. A lease expiry isn't invisible — it records a synthetic `LEASE_EXPIRED`
attempt in that same history, so the DLQ shows *why* a task took an extra lap.

Now stack two features that were each correct in isolation:

1. **Redrive** takes a dead task and re-enqueues it with its **policy reset** — it starts
   attempting again.
2. **Lease expiry** records a `LEASE_EXPIRED` attempt using the task's attempt number.

The bug: when we recorded the `LEASE_EXPIRED` attempt, we derived `attempt_no` from the task's
live `attempt_count`. For a fresh task that's fine. But a **redriven** task has already
accumulated attempt rows from its first life. Reset the counter on redrive, let the next attempt's
lease expire, and the `LEASE_EXPIRED` write computes an `attempt_no` that **already exists** in the
history from before the redrive — and slams straight into the `UNIQUE(task_id, attempt_no)`
constraint. The reclaim transaction aborts. The task can't be reclaimed. The one path that's
supposed to *rescue* stuck work was the path that jammed.

It's a lovely little bug because both features are individually correct; only their composition —
*a redriven task whose subsequent lease expires* — reveals the shared assumption that attempt
numbers are monotonic within a single logical life.

## The fix: derive the number from history, not from a counter

The fix was to stop trusting the live counter and instead **compute the next `attempt_no` from
the attempt history itself** — `MAX(attempt_no) + 1` for that task — so a redriven task's
`LEASE_EXPIRED` row always lands *after* everything already recorded, never on top of it. History
is the source of truth for "what number comes next," because history is the thing the constraint
is actually protecting.

The general lesson has outlived the specific bug: **when a monotonic sequence has a UNIQUE
constraint, derive the next value from what's already persisted, not from a mutable counter that a
feature like redrive is allowed to reset.** The counter and the history disagreed exactly at the
seam between two features, and the database — correctly — refused to let them.

## Why this is the architecture, not a detail

It would be easy to file all of this under "implementation." It isn't. The claim-and-lease model
*is* the reason rdq can make its headline promises:

- **Stateless workers** — because the claim lives in storage, a worker holds no authoritative
  state. Any worker can pick up any task.
- **Horizontal scaling** — because `SKIP LOCKED` (and its Redis/Mongo equivalents) hands
  concurrent workers disjoint tasks, "scale up" is "start another process."
- **Fault tolerance for free** — because the lease turns any crash into a reclaimable task, node
  death needs no special handling.
- **HA inherited, not invented** — because the only coordination point is the backend, rdq's
  availability is your Postgres/Redis/Mongo availability, and rdq adds nothing stateful of its own.

Every storage plugin has to clear the same bar — no double-claims, ever — which is why the SPI
ships with a **compliance test-kit** that includes a multi-worker chaos test: `kill -9` a worker
mid-processing and assert the task is reclaimed after its lease expires, with no double-execution.
A plugin that can't pass that isn't a plugin.

## Read on

- [Tasks, attempts & the lifecycle](/rdq/concepts/task-lifecycle/) — the full state machine.
- [Storage SPI & compliance kit](/rdq/concepts/storage-spi/) — what every backend must guarantee.
- [Architecture — one core, two hosts](/rdq/concepts/architecture/) — where claims fit in.

---

*rdq is open source under Apache-2.0 — [github.com/srjn45/rdq](https://github.com/srjn45/rdq).*
