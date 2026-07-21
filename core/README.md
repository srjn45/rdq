# `core/` — the rdq engine and frozen storage contract

Module `github.com/srjn45/rdq/core`. This is the language-neutral heart of rdq:
the task envelope, the storage service-provider interface (SPI), and the
compliance kit that proves a storage backend correct. Everything else — the
Postgres binding, the Go SDK, `rdq-server`, the CLI, and the Java SDK — is
written against the contract defined here.

> **Frozen as of milestone M1.** The wire envelope, the `spi.Storage`
> signatures, the error sentinels, and the golden JSON fixtures below are the
> spec every other module and language assumes. Treat a change to any of them as
> a contract break — see [`docs/design/01-wire-envelope.md`](../docs/design/01-wire-envelope.md)
> and [`docs/design/02-storage-spi.md`](../docs/design/02-storage-spi.md).

## Packages

| Package | What it is |
|---|---|
| [`envelope`](envelope/) | The wire model — `Envelope`, `Attempt`, `Error`, and the `Status` / `Outcome` enums — plus the canonical JSON codec (RFC-3339-millis timestamps, integer-ms durations, base64 payloads, ULID ids), unknown-field preservation, and validation/truncation helpers. |
| [`spi`](spi/) | The **frozen** `Storage` interface and its value types (`Claimed`, `ClaimToken`, `DLQFilter`, `Selector`, `Page`, `Cursor`, `Stats`, `Capabilities`) and error sentinels (`ErrStaleClaim`, `ErrNotFound`, `ErrStaleCursor`, `ErrIDConflict`). |
| [`compliance`](compliance/) | `Run(t, factory)` — the eight-invariant conformance suite a backend must pass. |
| [`memstore`](memstore/) | The in-memory reference `Storage`: correct, mutex-guarded, and the substrate the engine's tests run against. |

## Writing a storage plugin

Bringing rdq to a new datastore is three steps:

1. **Implement `spi.Storage`.** Every method's doc comment restates the
   invariant it must uphold. The mandatory floor is the whole interface;
   optional accelerations (native filter pushdown, `LISTEN/NOTIFY`, batch
   enqueue) are advertised through `Capabilities` and only remove latency —
   never change correctness.

2. **Run the compliance kit.** The kit is an ordinary importable package, so a
   plugin in a different module calls it from its own test:

   ```go
   func TestCompliance(t *testing.T) {
       compliance.Run(t, func() spi.Storage { return mystore.New(/* ... */) })
   }
   ```

   Each of the eight invariants (no-double-claim, fencing, lease-recovery
   counts, atomic transition, idempotent enqueue, lossless round-trip, redrive
   reset, stable pagination) runs as a named subtest, so a failure names the
   exact contract clause your backend violates.

3. **You're done.** When the kit is green the engine can drive your store —
   it assumes nothing beyond the SPI floor.

## Contract you must honor

- **The backend's clock is the time authority (G9).** Due-ness and lease expiry
  are evaluated against the backend's own "now" (e.g. Postgres `now()`); the
  engine supplies `next_attempt_at` values but never a "now". The kit never
  injects a clock — it uses short real leases and waits past them.
- **Every mutation is atomic.** A crash between any two calls leaves a task in a
  valid state: at worst retried after lease expiry (at-least-once, never lost).
- **Claims are fenced.** `ClaimDue` mints one `ClaimToken` per task; at most one
  is valid at a time. `ExtendLease` / `Reschedule` / `Complete` / `DeadLetter`
  reject any other token with `ErrStaleClaim` and change nothing.
- **Enqueue is idempotent within a queue** and returns `ErrIDConflict` for the
  same id in a *different* queue (G8).

## Golden fixtures — the cross-language contract

The JSON files under [`envelope/testdata`](envelope/testdata/) (mirrored in
[`compliance/testdata`](compliance/testdata/)) are the canonical wire bytes,
frozen here and replayed byte-for-byte by the Postgres binding (M2) and the Java
SDK (M7):

| Fixture | Covers |
|---|---|
| `envelope_full.json` | A fully-populated envelope with attempt history. |
| `lease_expired.json` | A `LEASE_EXPIRED` attempt with `error.type = "rdq.LeaseExpired"` (G7). |
| `error_type_go.json` | Go-produced `error.type` values per the G6 convention. |
| `unknown_fields.json` | Residual top-level and per-attempt fields that must survive a round-trip. |

## Building and testing

```sh
go build ./...
go vet ./...
go test ./...
```

All three must be green before this contract is frozen.
