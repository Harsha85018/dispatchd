# dispatchd

A distributed task scheduler in Go. Jobs are submitted over HTTP, stored in Postgres, and executed by worker processes that lease work from a server. Jobs can declare dependencies on other jobs, forming a DAG that the scheduler resolves before dispatching anything.

Built to work through the correctness problems that appear once execution is distributed: what happens when a worker dies mid-job, when a job outlives its lease, when an abandoned attempt finishes and reports a result nobody is waiting for, and when several server replicas start against an empty database at the same moment.

## Architecture

```
                      ┌──────────────┐
  external clients ──▶│  nginx (lb)  │──┐
                      └──────────────┘  │   ┌──────────┐
                                        ├──▶│ server ×3│──┐
  workers ─── DNS alias `servers` ──────┘   └──────────┘  │
    (skip the proxy: a hop here cost ~35%)        │       │
                                                  │  ┌───────────┐
                             /metrics ────────────┘  │ Postgres  │
                                  │                  │  (state)  │
                          ┌───────────────┐          └───────────┘
                          │ Prometheus    │
                          │   → Grafana   │
                          └───────────────┘
```

**Servers** are stateless. All job state lives in Postgres, so any replica can serve any request and replicas can come and go freely. Each runs a reaper that requeues jobs whose leases have expired.

**Workers** are separate processes holding no state. They lease a job, heartbeat to extend the lease while it runs, and report the outcome. A worker can be killed at any point and its work is recovered.

**Leasing** is a single statement using `FOR UPDATE SKIP LOCKED`, so concurrent workers each claim a different row instead of queueing behind one another. Dependency resolution happens in the same statement as a `NOT EXISTS` subquery over unmet dependencies.

A file-backed write-ahead-log store is also in the tree (`internal/store/store.go`) and is selected when `-db` is omitted. Postgres is the default for anything real; the WAL store is the earlier design, kept because the comparison between them is part of what this project documents.

## Correctness properties

Covered by tests in `internal/store/lease_test.go` and `cmd/worker/cancel_test.go`.

**Durability.** Job state is committed to Postgres before being acknowledged. The WAL store fsyncs every mutation and replays the log on startup.

**Exclusivity.** `SKIP LOCKED` under a row lock means two workers polling at the same instant can never receive the same job.

**Recovery.** Every lease carries an expiry. A worker that dies stops heartbeating, the lease lapses, and the reaper returns the job to the queue.

**Freshness.** Every lease carries a random token. A stale result from an abandoned attempt is rejected even when the *same worker* holds the current lease — a case worker ID alone cannot distinguish.

Three reapers run concurrently without coordination. `ReapExpired` is a single atomic `UPDATE ... WHERE status='leased' AND lease_expiry < now() RETURNING id`; under READ COMMITTED the loser of any race re-evaluates its `WHERE` against the committed row and matches nothing. Leader election would add a failure mode without buying anything.

## Bugs found while building this

Each was found by testing the system, not by reading the code, and each changed the design.

**Long jobs were being duplicated.** A job running longer than its lease got reaped out from under a healthy worker and handed to a second one. A 30-second job under a 10-second lease was guaranteed to run three times. Fixed with worker heartbeats that extend the lease while a handler runs, so the reaper only reclaims work from workers that have actually stopped.

**Stale results could overwrite live ones.** `Complete` validated the caller by worker ID. But a worker that lost a lease and then re-leased the same job passed that check carrying a result from the *previous* attempt, marking a job succeeded while another attempt was still in flight. Fixed with a per-lease token minted on every lease; a stale attempt carries a dead token and is rejected with a 409.

**Abandoned work kept running.** A worker that lost its lease finished the handler anyway, occupying a slot and burning retry attempts on a result it would discard. Handlers now take a `context.Context` that the heartbeat cancels on lease loss.

**Replicas raced each other creating the schema.** `CREATE TABLE IF NOT EXISTS` is not atomic against concurrent callers. All three replicas started together, all three found the table missing, all three tried to create it, and two died with a unique violation in `pg_catalog`. Migration now runs under a transaction-scoped advisory lock. This bug is invisible on a warm database — it only reproduces from an empty volume.

**A proxy hop on the hot path cost 35% throughput.** Routing worker traffic (lease, renew, complete — thousands of requests) through nginx alongside external traffic dropped sustained throughput from 1,876 to 1,204 jobs/sec. Workers now resolve a shared DNS alias and talk to a replica directly; nginx fronts external clients only.

## Observability

Every server exposes Prometheus metrics on `/metrics`, and `docker compose up` brings up Prometheus and a provisioned Grafana dashboard alongside the stack.

| Metric | What it answers |
|---|---|
| `dispatchd_jobs_submitted_total` / `_leased_total` / `_completed_total{outcome}` | Job lifecycle rates. Leases exceeding submissions means jobs are retrying. |
| `dispatchd_jobs{status}` | Queue depth, sampled from the database rather than tracked in memory, because depth is a property of the system and not of one replica. |
| `dispatchd_http_request_duration_seconds{path}` | Server-side latency per endpoint. Buckets start at 0.5ms — lease calls are sub-millisecond when healthy. |
| `dispatchd_leases_reaped_total` | Workers dying or overrunning their leases. Zero when healthy. |
| `dispatchd_leases_lost_total` | Stale results rejected by the token check. |

Two things worth knowing when reading the dashboard.

**Gauges use `max()`, not `sum()`.** Every replica samples the same database, so summing queue depth would multiply it by the replica count.

**Rate windows are `[15s]`, not the usual `[1m]`.** Load test bursts last a handful of seconds. Averaged over a minute, a 2,900/sec burst renders as ~350/sec — a factor of eight, and a very easy way to misread your own dashboard.

The first thing the metrics surfaced: during idle, six workers generated **109 lease requests for every one job handed out** — 99% of lease traffic returning `204 No Content`. That is the cost of polling, and it is invisible in logs.

## Performance

All figures from `cmd/loadtest`: 10,000 no-op jobs, six workers, Postgres, Docker on an M-series Mac, each run starting from an empty database and submitting directly to a replica so the proxy is not part of the measurement.

**Headline: ~2,800 jobs/sec sustained end-to-end**, submitted at ~5,200/sec, with zero duplicate executions.

Run benchmarks with `docker compose stop prometheus grafana`. Scraping three targets every two seconds while Grafana re-renders six panels takes a measurable bite out of throughput on a single machine — roughly half, in one measured case.

### What "sustained" means

Execution overlaps submission — workers start draining the moment the first job lands. An earlier version of the harness timed only the window *after* submission finished and divided the full job count by it, which overstated throughput by roughly 2×. "Sustained" here is total jobs divided by the wall-clock span from earliest submission to latest completion. It is the smaller and more defensible number.

Figures elsewhere in this document that predate that fix are labelled; they remain valid for comparison *against each other* but are not comparable to the sustained numbers.

### One server vs three

| | 1 server, 6 workers | 3 servers, 6 workers |
|---|---|---|
| submit | 5,196/sec | 3,882/sec |
| sustained | **2,828/sec** | **2,164/sec** |
| p99 | 1.705s | 2.245s |

Three replicas are 23% *slower*. This is worth stating plainly because the expectation runs the other way.

The mechanism is visible in the submit column. Submission went to `server1` directly in both runs — identical path, same single process accepting every request. Throughput still dropped 25% purely because servers 2 and 3 existed. They handled none of that traffic; they only competed for CPU with Postgres and the workers.

Everything here runs on one laptop. Horizontal scaling needs horizontal hardware. On a single host, replication buys availability, not throughput.

### What replication does buy

`./failover.sh` submits 30,000 jobs and kills two of three replicas once draining starts.

| | |
|---|---|
| completed | 30,000 / 30,000 |
| failed | 0 |
| attempts | 30,002 |
| p99 latency | 4.904s |
| max latency | 34.833s |

Two duplicate attempts in 30,000 — a 0.007% duplicate rate — and those two are the jobs that were leased by workers connected to the dying servers. The ~30-second gap between p99 and max is exactly the lease duration: those two jobs waited out their leases, were reaped, and were re-run by a surviving worker.

Recovery time is therefore bounded by lease duration, which is the real tuning knob. A shorter lease recovers faster but raises the risk of reaping work that is still healthy — the exact failure mode described in the first bug above.

### Earlier measurements (superseded metric)

These used the overstating drain-window figure. They are internally consistent and the conclusions hold, but the absolute numbers are not comparable to the sustained figures above.

**Postgres vs the file-backed WAL store.** Swapping a single global mutex plus one fsync per operation for `SKIP LOCKED` on row-level locks roughly doubled how well the system scaled with workers: going from 2 to 6 workers improved throughput 35% on the WAL store and 95% on Postgres. p99 halved, from 1.02s to 475ms.

**A pending-job index.** `Lease` originally scanned every job in the store on each call, so its cost grew with total store size rather than with pending work. Replacing that with a maintained index of pending IDs improved throughput ~14% and flattened the degradation curve — at 5,000 stored jobs, throughput went from 15% below empty to about 8% below. (Postgres later made this moot: a partial index on `status = 'pending'` does the same job, maintained by the database.)

**Docker's filesystem flattered the WAL store.** The same binaries ran ~10× faster on submit inside Docker than natively, because the WAL sat on a volume in Docker Desktop's Linux VM where `fsync` does not reach physical storage the way it does on the host SSD. Faster, and less durable. This caveat does not apply to the Postgres numbers, which are real transactional commits.

## Running it

Requires Docker, or Go 1.27+ for the file-backed store.

```bash
docker compose up --build
```

That brings up Postgres, three server replicas, nginx on `:8080`, six workers, Prometheus on `:9090` and Grafana on `:3000`. `server1` is also published directly on `:8081`.

Grafana opens straight onto the dispatchd dashboard — anonymous access is enabled because this is a local demo stack.

```bash
# submit a job
curl -X POST localhost:8080/jobs -d '{"id":"job-1","type":"extract"}'

# a job that waits for another to succeed
curl -X POST localhost:8080/jobs \
  -d '{"id":"job-2","type":"transform","depends_on":["job-1"]}'

# check status
curl localhost:8080/jobs | python3 -m json.tool
```

Single-process mode, no database:

```bash
go test ./... -race
go build -o bin/server ./cmd/server
go build -o bin/worker ./cmd/worker
go build -o bin/loadtest ./cmd/loadtest

./bin/server -lease 30s          # omitting -db uses the file-backed WAL store
./bin/worker -id worker-a
```

### Benchmarks

```bash
docker compose stop prometheus grafana   # monitoring costs throughput on one host

./compare.sh     # 1 vs 3 server replicas, clean database each run
./failover.sh    # kill 2 of 3 replicas mid-run, verify no job loss
./sweep.sh       # throughput across lease durations   (WAL store)
./growth.sh      # throughput as the store grows       (WAL store)
```

### API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/jobs` | Submit a job |
| `GET` | `/jobs` | List all jobs |
| `GET` | `/jobs/{id}` | Fetch one job |
| `POST` | `/lease` | Worker claims a ready job |
| `POST` | `/renew` | Worker extends its lease |
| `POST` | `/complete` | Worker reports an outcome |
| `GET` | `/healthz` | Liveness, plus which replica answered |
| `GET` | `/metrics` | Prometheus exposition |
