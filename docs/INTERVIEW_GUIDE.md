# TaskForge interview guide

## Explain the project in one minute

TaskForge accepts background jobs over HTTP and persists them in PostgreSQL. A C++ scheduler chooses eligible work and assigns it to registered workers without exceeding queue or worker capacity. C++ workers use bounded thread pools to run allowlisted handlers. Leases, heartbeats and increasing attempt numbers let the system recover after failures and reject late results. Workflows add dependency ordering; a React dashboard displays real state and execution history.

The main engineering work is the ownership protocol: submission, assignment, execution, renewal, completion and recovery each have a defined database transaction boundary. The system targets at-least-once execution and explicitly does not promise exactly-once external effects.

## Why each technology exists

| Technology | Responsibility and reason |
| --- | --- |
| C++20 | Scheduler/worker lifetime management, bounded thread pools, stop tokens and explicit concurrency |
| CMake | Portable compiler/dependency configuration and sanitizer builds |
| libpq | Direct parameterized PostgreSQL communication from C++ |
| PostgreSQL | Durable records, row locks, transactional state transitions, constraints and fencing |
| Node/Express/TypeScript | HTTP contracts, authentication, orchestration submission and manageable API code |
| Zod | Reject malformed payloads, unsafe handler types and invalid graphs before persistence |
| React/Vite/Tailwind | Operations UI, efficient local build and consistent responsive styling |
| React Flow | Inspect graph execution and open per-node results |
| Docker Compose | Reproducible services with independent credentials and internal networking |
| Prometheus text format | Integrates real state metrics with standard monitoring tools |
| Redis/gRPC/Kafka | Deferred: none is required for the current durable contract |

## Follow one job

1. The API hashes the bearer token and identifies the project. It validates the queue and payload.
2. A project advisory lock serializes quota/idempotency checks. The API inserts the job and audit record, then commits before replying.
3. The scheduler lists live non-draining workers. `dispatch_job` locks a worker, checks its active leases, locks an eligible queue, rechecks queue capacity, and locks one eligible job.
4. The assignment transaction increments the attempt number, inserts an ASSIGNED attempt, gives the job RUNNING ownership and a lease/deadline, and commits.
5. `next_assignment` locks a candidate job and conditionally changes the attempt from ASSIGNED to RUNNING. If that conditional update affects no row, nothing is returned. Locking the job alone would not refresh an old snapshot of the joined attempt.
6. A worker lane runs the fixed handler. Its checkpoints renew only its current unexpired lease; sleep handlers check about every 50 ms and renew at most every 400 ms.
7. `finish_job` locks the job and accepts the outcome only if the worker, attempt number, state and unexpired lease still match.
8. A successful result is committed to PostgreSQL. An error schedules retry or dead-lettering. Failure to confirm completion is logged as a database error rather than success.

## Thread pool

`services/core/include/taskforge/thread_pool.hpp` owns joinable `std::jthread` objects. A mutex protects a deque, and a condition variable suspends idle threads. Pending work is bounded to twice the thread count; submit returns false when full or shutting down. Shutdown rejects new submissions, clears pending callbacks, requests stop on active lanes, wakes waiters and joins all threads before destroying shared state.

The worker submits one long-lived database polling lane per pool thread. Each lane owns its libpq connection; connections are never shared concurrently across threads. This is a fixed execution pool, not an unbounded thread per job. Handlers do not launch detached threads.

Queue push/pop is O(1) under the mutex; memory is O(T) for T threads and bounded pending callbacks. Thread scheduling and handler cost dominate actual runtime.

## Locks, leases and retries

Worker/queue/job locks make assignment atomic. A queue row is a common serialization point for its capacity count. Multiple schedulers use SKIP LOCKED to avoid waiting on candidates owned by another transaction. The attempt-status conditional update separately makes assignment acceptance atomic between worker lanes.

The monotonic attempt number is a fencing token: a worker holding attempt 1 cannot finish attempt 2. Lease expiry also rejects a late result before a replacement is assigned. Heartbeats answer whether a worker may receive more work; per-job lease renewal answers whether a specific execution still owns work. These are related but different mechanisms.

Maintenance and workflow cancellation acquire the same transaction advisory lock before locking workflow/job rows. This avoids maintenance's jobs-then-workflow order deadlocking cancellation's workflow-then-jobs order. Project quota locks are separate and shared by job submission, workflow creation and manual retry.

Automatic backoff is 500 ms times 2 to the attempt-minus-one, capped at five minutes, with jitter between half and the full base. Max attempts counts assignments, including lost assignments to a worker that looked live briefly after a crash. Manual standalone retries grant three more assignments without resetting fencing.

## DAGs

Kahn's algorithm validates workflows. Build an adjacency list and indegrees, visit each zero-indegree node, reduce child indegrees and append newly eligible nodes. If visited count differs from node count, a cycle exists. A queue index avoids repeatedly shifting an array, preserving O(V+E) time and O(V+E) space.

The validated graph and all nodes/edges are inserted atomically. Drafts do not execute. After a run starts, a pending node becomes READY only if no predecessor is incomplete. Completion of multiple parents cannot duplicate the node because promotion is a conditional row update, and maintenance is serialized. Failed/cancelled dependencies cancel pending descendants. The current version orders nodes but does not automatically feed outputs into their payloads.

## Files to open during an interview

| File / symbol | What it demonstrates |
| --- | --- |
| database/migrations/001_execution.sql: dispatch_job, finish_job, maintain_jobs | Capacity, fencing and recovery transactions |
| database/migrations/003_acceptance_and_retry_quota.sql: next_assignment, retry_job | Fresh-state acceptance and quota-safe retries |
| services/worker/main.cpp: lane, execute | Execution lifecycle and durable acknowledgement |
| services/core/include/taskforge/thread_pool.hpp | Bounded work, synchronization and RAII shutdown |
| services/core/include/taskforge/handlers.hpp: ExecutionContext | Cooperative renewal/cancellation and a safe registry |
| packages/contracts/src/index.ts: topologicalOrder | Graph validation and request contracts |
| apps/api/src/jobs.ts | Project scope, idempotency and transactional quota |
| apps/api/src/operations.ts | Real metrics, bounded streams and token revalidation |
| apps/api/src/pagination.ts | Timestamp-accurate cursor pagination |
| benchmarks/run.mjs | Reproducible HTTP workload measurement |

## Common questions

**Why not exactly once?** A worker can perform an external effect and lose the database connection before recording completion. Retrying preserves progress but may repeat the effect. Exactly-once effects require a shared transaction or an external system's idempotent operation, not merely a queue flag.

**Does submission idempotency solve worker duplicates?** No. It prevents two job records for the same project/key/request. Attempt fencing protects the durable result; application-level idempotency protects external effects.

**What if two workers finish simultaneously?** Ownership/attempt checks and the job row lock allow only the current unexpired attempt's durable completion. Even before that, conditional attempt acceptance prevents two lanes from legitimately accepting the same current assignment.

**What if PostgreSQL is down?** New submissions cannot be acknowledged. Existing workers cannot renew/confirm results and must stop claiming success. Once connections return, durable pending work and expired leases are reconciled. Database HA is not implemented.

**Can two schedulers overfill a queue?** They must lock the same queue row and recheck its active lease count before granting another. Worker capacity is similarly serialized by the worker row lock.

**Is scheduling fair?** Queues get round-robin opportunities by last dispatch time; jobs within a queue are priority first. There is no weighted per-tenant fairness or starvation prevention for low-priority jobs under continuous higher-priority arrivals.

**What is the scheduler's complexity?** Database indexes support ready lookup, but correlated active-count/eligibility queries scan qualifying rows. It is not honest to claim O(log N) end-to-end scheduling from a B-tree alone. At scale, measure query plans and consider maintaining transactional counters or sharded coordination.

**What did you actually measure?** Four 80-job samples, documented with host details and raw output. Two workers observed 46.36 HTTP-completions/s on synthetic no-ops in one short run. These are not sustained-capacity or production claims. A 10,000-number sum sample also checked a real result.

**How were failures tested?** A real worker was killed mid-execution, a scheduler was killed while another continued, PostgreSQL restarted with five pending jobs, competing schedulers processed 30 jobs under queue limit 2, and stale attempts/timeouts/DAG order were checked against real database state. Tests are local rather than published, at the repository owner's request.

## Demonstration commands

Start with `docker compose up -d --build --scale worker=2 --scale scheduler=2`. Create a project, connect the dashboard and submit several sleep jobs; inspect queue occupancy and workers.

For a deterministic worker-crash demo, scale to one worker, submit a 20-second sleep with `timeout_ms=60000` and `max_attempts=3`, wait until its attempt has started, then run `docker compose kill -s SIGKILL worker`. Restart with `docker compose up -d --scale worker=1`. Watch the new fenced attempt after lease recovery. Effects can repeat; the attempt log explains why.

Kill one of two scheduler containers with `docker kill taskforge-scheduler-1` and submit another job; the second scheduler continues. Restart the killed container afterward. The dashboard's workflow creation dialog supplies a diamond graph. Run it and inspect node attempt timestamps to demonstrate ordering. Create a fail handler with `fail_until_attempt=1` to demonstrate successful retry, or a large threshold with max_attempts=2 to demonstrate dead-lettering.

## Defensible resume wording

Implemented a PostgreSQL-backed distributed job queue with a C++20 scheduler and bounded worker pools, an authenticated TypeScript API, and a live React workflow dashboard; exercised lease fencing, worker recovery, competing schedulers and DAG ordering with local fault-injection checks.

Do not add exactly-once guarantees, production traffic figures, a published automated suite, Redis/gRPC/OpenTelemetry integration, or sustained throughput numbers to this description: those are not implemented or established here.
