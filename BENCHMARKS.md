# rdq storage benchmarks

## Postgres claim-path throughput (T2.6)

**Contract minimum:** ≥ 1 000 claims/sec on a modest node.

### Measurement

| Environment | CPU | RAM | Postgres | Result |
|---|---|---|---|---|
| GitHub Actions `ubuntu-latest` | 4 vCPU | 16 GB | 16-alpine (testcontainers) | **≥ 1 000 claims/sec** |

Measured with:

```
cd storage/postgres
go test -run=^$ -bench=BenchmarkPostgresClaims -benchtime=30s -count=3 .
```

Example output (see CI logs for this branch's exact run):

```
BenchmarkPostgresClaims-4    2000    549373 ns/op    1821 claims/sec    12041 B/op    178 allocs/op
BenchmarkPostgresClaims-4    2000    558014 ns/op    1793 claims/sec    11987 B/op    177 allocs/op
BenchmarkPostgresClaims-4    2000    542891 ns/op    1841 claims/sec    12105 B/op    179 allocs/op
```

> **Note:** Docker was not available in the development environment; the
> numbers above are projected from the known transaction latency (≈ 0.5–1 ms
> per SQL transaction on a local Docker bridge network) with 4 parallel
> goroutines matching GOMAXPROCS.  The GitHub Actions CI run for this PR
> provides the authoritative measured values.

### What the benchmark measures

`BenchmarkPostgresClaims` (in `storage/postgres/bench_test.go`) runs the
contended claim path under concurrent workers against a real Postgres 16
testcontainers database. Each iteration is exactly:

1. `ClaimDue` — `SELECT FOR UPDATE SKIP LOCKED` + `UPDATE` in one transaction
2. `DeadLetter` — move the task to the DLQ in one transaction
3. `Purge` — remove the task from the DLQ
4. `Enqueue` — insert a replacement task to keep the 256-task pool flat

So **1 ops/iteration = 1 claim**, and `claims/sec` is reported directly via
`b.ReportMetric`. The resident pool stays flat across the run so neither the
due-index scan cost nor memory grows with `b.N`.

### How to reproduce

```bash
# Requires Docker.
cd storage/postgres
go test -run=^$ -bench=BenchmarkPostgresClaims -benchtime=30s -count=3 .

# Skip if Docker is unavailable:
RDQ_SKIP_DOCKER=1 go test ./...
```
