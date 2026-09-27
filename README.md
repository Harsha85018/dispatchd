# dispatchd

A distributed task scheduler in Go. Jobs are submitted over HTTP, stored durably in a write-ahead log, and executed by worker processes that lease work from a central server. Jobs can declare dependencies on other jobs, forming a DAG that the scheduler resolves before dispatching.

Built to explore the correctness problems that show up once work is distributed: what happens when a worker dies mid-job, when a job outlives its lease, or when an abandoned attempt finishes and tries to report a result nobody is waiting for.

## Architecture

```
  client ──POST /jobs──▶  server  ◀──lease/renew/complete──  worker ×N
                            │
                            ├── in-memory job map + pending index
                            ├── write-ahead log (fsync per write)
                            └── reaper (requeues expired leases)
```

The **server** owns all job state. It exposes an HTTP API, persists every state change to a write-ahead log before applying it in memory, and runs a reaper that requeues jobs whose leases have expired.

**Workers** are separate processes. They poll the server for work, receive a job plus a lease token, heartbeat to extend the lease while the job runs, and report the outcome. They hold no state — a worker can be killed at any point and the system recovers.

## Correctness properties

Each of these is covered by tests in `internal/store/lease_test.go` and `cmd/worker/cancel_test.go`.

**Durability.** Every mutation is written to the WAL and fsynced before being applied in memory. On startup the server replays the log to rebuild state, so a job submitted before a crash is still there after it.

**Exclusivity.** `Lease` selects and marks a job under a single write lock, so two workers polling concurrently can never receive the same job.

**Recovery.** A lease carries an expiry. A worker that dies stops heartbeating, its lease expires, and the reaper returns the job to the queue for another worker.

**Freshness.** Each lease carries a random token. A stale result from an abandoned attempt is rejected even when the same worker holds the current lease — which worker ID alone cannot distinguish.

## Bugs found while building this

These were found by testing the system rather than by reading the code, and each one changed the design.

**Long jobs were being duplicated.** A job that ran longer than its lease got reaped out from under a perfectly healthy worker and handed to a second one. A 30-second job under a 10-second lease was guaranteed to run three times. Fixed by having workers heartbeat to extend the lease while a handler runs; the reaper now only reclaims work from workers that have actually stopped.

**Stale results could overwrite live ones.** `Complete` validated the caller by worker ID. But a worker that lost a lease and then re-leased the same job passed that check with a result from the *previous* attempt, marking a job succeeded while another attempt was still in flight. Fixed with a per-lease token minted on every lease; a stale attempt carries a dead token and is rejected with a 409.

**Abandoned work kept running.** A worker that lost its lease finished the handler anyway, occupying a slot and burning retry attempts on a result it would discard. Handlers now take a `context.Context` that the heartbeat cancels on lease loss.

## Performance

Measured with `cmd/loadtest`, submitting 1,000 no-op jobs and timing until every job reaches a terminal state. Numbers are from an M-series Mac, so treat them as relative rather than absolute.

| Setup | Submit | Drain | p99 latency |
|---|---|---|---|
| Native, 2 workers | 307 jobs/sec | 182 jobs/sec | 7.9s |
| Docker, 2 workers | 3,024 jobs/sec | 960 jobs/sec | 1.06s |
| Docker, 6 workers | 2,250 jobs/sec | 1,298 jobs/sec | 1.02s |

Two things worth reading carefully here.

**The Docker numbers are not a code improvement.** The same binaries run ~10× faster on submit inside Docker, because the WAL lives on a volume in Docker Desktop's Linux VM where `fsync` does not reach physical storage the way it does on the host SSD. That is faster and less durable. The native numbers are the honest ones for a crash-safe configuration.

**Scaling workers is sublinear.** Tripling workers (2 → 6) improved drain throughput by ~35%, and submit throughput not at all, since submission never touches a worker. The server is the bottleneck: every lease, renewal and completion serializes on one mutex and one fsync.

### An optimization with before/after

`Lease` originally scanned every job in the store on each call to find a pending one, so lease cost grew with total store size rather than with the amount of pending work. Replacing that scan with a maintained index of pending job IDs:

| Jobs in store | Before | After |
|---|---|---|
| 0 | 165/sec | 182/sec |
| 2,000 | 147/sec | 167/sec |
| 5,000 | 141/sec | 167/sec |

Throughput improved ~14% across the board, and the degradation curve flattened: before, throughput at 5,000 stored jobs was 15% below empty; after, ~8%, most of which is likely the load test's own polling of `GET /jobs` growing more expensive as the store fills.

## Running it

Requires Go 1.27+ (or just Docker).

```bash
# tests
go test ./... -race

# build
go build -o bin/server ./cmd/server
go build -o bin/worker ./cmd/worker
go build -o bin/loadtest ./cmd/loadtest

# run: server in one terminal, workers in others
./bin/server -lease 30s
./bin/worker -id worker-a
./bin/worker -id worker-b
```

Or with Docker:

```bash
docker compose up --build
docker compose up -d --scale worker=6   # scale workers without restarting the server
```

### Submitting jobs

```bash
# a single job
curl -X POST localhost:8080/jobs -d '{"id":"job-1","type":"extract"}'

# a job that waits for another to succeed
curl -X POST localhost:8080/jobs \
  -d '{"id":"job-2","type":"transform","depends_on":["job-1"]}'

# check status
curl localhost:8080/jobs | python3 -m json.tool
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
| `GET` | `/healthz` | Liveness |

### Benchmarks

```bash
./sweep.sh    # throughput across lease durations
./growth.sh   # throughput as the store grows
```

## Known limitations

**Single server.** All state lives in one process. The server is a single point of failure and the throughput ceiling. Moving state to Postgres would allow multiple replicas and remove the in-memory bottleneck.

**The WAL only grows.** There is no compaction or snapshotting, so the log grows without bound and startup replay time grows with it.

**Lock granularity.** Every store operation takes one global mutex and holds it across an fsync. Batching WAL writes with a periodic flush would raise the ceiling significantly, at the cost of a small window where an acknowledged write could be lost.

**Polling, not push.** Idle workers sleep and poll rather than waiting on a long-lived connection, which adds latency when work arrives during a sleep and wastes requests when the queue is empty.

**One unexplained observation.** An early load test produced 44 spurious failures and 473 duplicate executions on a handler that cannot fail. It has not reproduced across subsequent runs spanning lease durations from 5s to 60s and store sizes up to 6,000 jobs. The most likely explanation is stray worker processes left over from earlier testing, but that was never confirmed, so it is recorded here rather than explained away.
