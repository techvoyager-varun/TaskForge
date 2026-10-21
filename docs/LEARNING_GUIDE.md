# Learn, debug and modify TaskForge

## 1. Run the actual system

Run the README Compose setup and create a project with the admin CLI. Keep its token private. Open the dashboard and submit a sum with numbers [2,3,7]. The result should be 12. Inspect the job, attempts and logs. Restart a worker: the durable job record remains in PostgreSQL.

Start with `packages/contracts/src/index.ts`. A handler name selects one predefined payload shape. Unknown fields, cycles, excessive graphs and arbitrary command names are rejected. Add input validation before expanding a handler contract.

## 2. Follow one HTTP transaction

Read `apps/api/src/app.ts`, `jobs.ts` and `database.ts`. Authentication hashes a project token and looks up the project. SQL queries scope resources to that project. Submission takes a project advisory lock, checks quota and queue ownership, inserts a job and audit record, then commits. An idempotency key plus canonical request hash allows a client to retry an uncertain HTTP response without creating another job.

Try the same key twice with the same body, then a changed body. Expect the same job, then HTTP409. An empty key is rejected with HTTP400. These are separate concerns from worker-side repeated effects.

## 3. Understand the tables

Read migration 001 for projects, queues, workers, jobs and attempts. A job describes current durable state; attempts preserve separate execution grants. FAILED/TIMED_OUT are attempt outcomes, while the job may enter RETRY_WAIT or DEAD_LETTERED. Constraints and triggers reject illegal transitions even when a C++ process calls the database directly.

Migrations are immutable after application. The migration ledger stores their checksums. Add a numbered migration rather than editing a deployed one. Migration 003 contains important procedure corrections; reading only migration 001 misses the currently installed definitions.

## 4. Trace a C++ assignment

`services/scheduler/main.cpp` repeatedly invokes PostgreSQL dispatch and maintenance. The database locks eligible worker and queue rows, rechecks capacity and assigns a READY job. `next_assignment` accepts only an ASSIGNED attempt and must successfully change that exact row to RUNNING. Two concurrent snapshots alone are not sufficient evidence of ownership.

Each worker lane has its own `Database` object. `thread_pool.hpp` bounds pending work and owns jthreads. `runtime.hpp` carries assignment/lease/stop state. `handlers.hpp` contains the fixed registry. In `database.hpp`, async libpq plus poll bounds connect/query I/O, and results are drained before the connection is reused. No scheduler rewrite or Kafka claiming loop is involved.

## 5. Reason about failure

Kill a worker after a sleep attempt starts. The external work may have partially run. Lease expiry and backoff permit a later attempt; the attempt number rejects old completion/renewal. Recently dead workers can still be eligible until their heartbeat ages out, so recovery can involve more than one wasted assignment.

A server-side statement timeout does not bound a silent network peer. The public TCP blackhole test accepts/forwards the initial database connection, then drops traffic while keeping sockets open. The C++ client must throw its own deadline instead of hanging. DNS resolution still lies outside this socket deadline.

Cancellation is cooperative. Built-in handlers periodically renew/check ownership and stop tokens. If you add a handler with an external effect, use a stable job ID for business idempotency. Never add arbitrary shell execution to the public payload contract.

## 6. Follow a workflow

`workflows.ts` validates a DAG and writes draft nodes/edges. Starting it releases eligible roots. Maintenance releases a child only after all parents have completed; failed/cancelled ancestors prevent descendant execution. Workflow cancellation shares the maintenance advisory-lock ordering. Retry creates a new execution and preserves root lineage.

The public diamond test verifies parallel predecessors and join ordering using actual attempt timestamps. Use it to understand what must remain true when changing orchestration.

## 7. Learn the Redis boundary

`redis.ts` owns one reconnecting client, atomic Lua admission and cache counters. Offline buffering is disabled and command/queue limits prevent indefinite work buildup. Redis failure returns503 at admission; a successful PostgreSQL job does not disappear.

Migration 004 advances a job version for job and attempt changes. Job detail first validates project ownership/current version in PostgreSQL, then looks up that version's cache key. A miss reads job+attempts in one SQL snapshot and caches it. A stale writer populates an old version, which is no longer current. Active TTL5s and terminal TTL60s bound retention. This trades one cheap database read for avoiding the larger detail snapshot; it is not a database outage cache.

In `tests/integration/redis.test.ts`, inspect the two API replicas, exact concurrent quota, wrong-project entry, real TCP outage/reconnect and noeviction/OOM check. Do not replace these with mocks when changing Redis failure behavior.

## 8. Learn the outbox boundary

Migration 005 captures lifecycle changes in the same transaction as job/workflow updates. Compact events exclude payload/result/token material. `publisher.ts` claims rows and sends them to Kafka. Inspect the ack-loss injection in the Kafka integration test: Kafka accepts real records, then the simulated publisher failure prevents delivered_at from being written. A later publisher repeats the records.

`pipeline.ts` inserts an inbox event UUID and derived effects in one SQL transaction. Only a newer state_version replaces a projection. Handler success precedes Kafka offset acknowledgement. Invalid schemas get a persistent marker; SQL transaction failures roll back and retry. The consumer never writes jobs.

Replay uses another group and namespace. KAFKA.md explains the difference between Kafka retention replay and a repeatable-read snapshot of retained PostgreSQL events. Rebuild analytics without resetting live processing.

## 9. Read measurements accurately

The existing Overview/Jobs/Workers/Workflow views use live API data. `Infrastructure.tsx` adds Redis readiness/hit counts, project outbox, shared Kafka lag, dispatches, queue wait and attempt execution p95. Missing lag is unavailable, not0. A broker connection is not proof of a running consumer; lifecycle heartbeats also matter.

Cache counters are per API process. Outbox/analytics counts are project scoped. Broker lag and pipeline heartbeat/error status are deployment operational data. The analytics projection can retain historical terminal jobs after source-job retention, so it is not a second current-state authority.

## 10. Run and extend verification

```sh
npm ci
npm test
npm run typecheck
npm run build
docker compose -f compose.test.yml up --build --abort-on-container-exit --exit-code-from tests
```

The Docker suite builds C++/GoogleTest, starts isolated PostgreSQL/Redis/Kafka, migrates and runs real process/API/fault assertions. It requires a test-named database and truncates test fixtures. Never point it at a real project database. CI runs the same public suite. TESTING.md explains sanitizers and unsupported runtime reporting.

For a change, identify the invariant first, add a regression that can fail for the relevant behavior, then modify the smallest relevant boundary. A scheduler change should preserve fencing/capacity tests; an event change should preserve rollback, deduplication and replay; an API change should preserve tenant and input checks. Do not remove assertions just because timing becomes inconvenient.

## 11. Measure before optimizing

`benchmarks/audit.ts` runs in the isolated test stack with real C++ processes and HTTP submissions. It compares worker/scheduler counts, records percentiles/contention samples, times hits/forced misses and event publishing, and kills a worker. Raw JSON contains machine details and limitations. One short run is not proof of linear scaling. Queue/worker polling and transactional event writes add latency; optimize only after identifying a repeatable bottleneck.

## 12. Bound your production claims

Read FINAL_ENGINEERING_REPORT.md and RESUME_GUIDE.md. This implementation demonstrates useful invariants but still needs replicated infrastructure, authenticated encrypted Kafka networking, tested backups, coordinated event retention, larger performance studies and deployment security work. Explain what a successful test proves and what its environment does not cover.
