# TaskForge final engineering report

Audit date: 21 October 2026. Scope: improve the existing PostgreSQL/C++ execution engine in place, publish reproducible verification, integrate Redis/Kafka, measure behavior and document actual limits. All work was performed in E:\TaskForge. The latest audit request explicitly superseded the earlier local-only testing preference.

## Architecture and preserved behavior

The Express/TypeScript API authenticates hashed project tokens, validates predefined handler contracts and commits requests to PostgreSQL. Custom C++20 schedulers coordinate worker/queue/job row locks and durable attempt grants. Bounded C++ worker pools execute handlers, renew leases and fence completion by worker/attempt/lease. PostgreSQL remains authoritative for jobs, retries, cancellation, DAGs and results.

Redis shares atomic API admission counters and caches job/attempt snapshots under PostgreSQL versions. Lifecycle triggers append an outbox in the same SQL transaction as state transitions. A separately restartable publisher sends committed records to Kafka; a consumer group maintains an idempotent SQL analytics projection. Consumers never assign jobs or change authoritative job state. The existing React dashboard now includes actual Redis/Kafka and scheduling measurements.

See architecture/execution-model.md, REDIS.md and KAFKA.md. Core scheduler/worker algorithms and fixed handler allowlists were preserved; no queue framework or competing Kafka scheduler was introduced.

## Audit findings and confirmed changes

| Finding                                                                                                | Change                                                                                                                                          | Evidence                                                                                   |
| ------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| Empty Idempotency-Key bypassed truthy validation and caused a database CHECK error/HTTP503             | Validate every supplied header, including empty strings; reject with400                                                                         | The public API regression failed before the fix and passed afterward                       |
| Blocking libpq could wait indefinitely on an established silent TCP peer despite SQL statement timeout | Nonblocking libpq connect/send/flush/consume, monotonic3s connect and5s query deadlines, protocol result draining and unusable-connection guard | Real TCP blackhole exceeded8s before the fix; afterward it terminates near5s               |
| Existing automated verification depended on ignored local files                                        | Public CTest/GoogleTest, contract, SQL, HTTP, DAG, process and failure tests; CI executes them                                                  | Isolated Docker suite runs without .local credentials/artifacts                            |
| API shutdown waited for long-lived SSE streams                                                         | Abort active SSE responses before HTTP/SQL shutdown; handle requests already ending                                                             | Concurrent ten-stream quota and shutdown regression passed                                 |
| Shared Redis cache eviction could reset live admission counters                                        | noeviction with TTL expiry; fail closed on quota-command/OOM failure                                                                            | Concurrent replicas, stale entries, outage/reconnect and memory-exhaustion regression      |
| Broker admin reachability could mark a stopped consumer healthy                                        | Consumer lifecycle/heartbeat state plus observed broker/SQL availability; close waits for pending health observation                            | Stopped consumer reports unavailable with a reachable broker; restart resumes projection   |
| A positive decimal larger than SQL bigint could pass event validation and block consumption            | Bound schema versions to positive signed bigint; quarantine invalid schemas                                                                     | Oversized version and unknown schema become durable poison markers; later records continue |
| Persistent Kafka directory initially lacked non-root ownership                                         | One-shot named-volume directory ownership initializer                                                                                           | Full persistent Compose broker starts as UID1000 and survives stop/start                   |

Independent focused review found no additional confirmed critical bug in existing claim/fencing, queue-capacity, lease or DAG lock coordination. Earlier assignment-acceptance, cancellation lock-order and retry-quota fixes remain in migration003 and retain regressions. This review is evidence about inspected paths, not a proof of all possible interleavings.

## Redis behavior

One Lua command increments a fixed-window IP counter and sets its expiry atomically across API replicas. Production limit:300 requests/IP/60s. IP keys are hashed. Proxy headers are not trusted by default. No offline buffering, bounded command queue,400ms commands and capped reconnect delays bound API work during outages. Admission returns503 when Redis cannot confirm a quota command; already committed jobs continue in C++.

Job detail first checks current PostgreSQL project ownership/state_version. A miss obtains job and attempts in one SQL snapshot. Keys include project/job/version, preventing late old writers from replacing current state. Every job/attempt mutation advances the version, including lease metadata. Active TTL5s, terminal TTL60s. Foreign/corrupt entries are misses; cache failures bypass caching. PostgreSQL is still required for every detail read. Redis is private, authenticated in the main stack and configured not to evict counters; it contains cached payload/result data and must remain trusted. Restart resets ephemeral quotas. Fixed windows permit boundary bursts.

## Kafka delivery and projections

Schema-versioned events cover creation/readiness/assignment/start/completion/failure/retry/cancellation, workflow transitions and worker-offline detection. Envelopes omit raw payload/result/error/token material. Existing jobs/workflows receive current-state snapshots during migration.

Outbox claims use SKIP LOCKED, random claim tokens and30s leases. Kafka acks=all precedes the conditional delivered_at update. Retry backoff caps at60s. Crash/ack loss can duplicate delivery; producer idempotence does not close the PostgreSQL/Kafka transaction gap. Aggregate keys choose one of three partitions, but concurrent publishers/backoff can reorder versions. The consumer inbox, monotonic projection, event counts and durable position commit atomically before Kafka acknowledgement. Duplicate UUIDs cannot repeat those SQL effects, and older unique versions cannot regress derived state.

Schema-invalid messages receive durable failure markers; their original bytes remain within Kafka retention. SQL transaction failures roll back and retry. Consumer lag is observed from Kafka end offsets versus durable projection positions; unavailable lag is null. Live consumer health requires lifecycle/heartbeat evidence. Project outbox/projection/counts are scoped; broker lag and process heartbeat/error status are shared operational data.

Replay within retention uses a fresh group/namespace. A separate CLI can republish a consistent repeatable-read snapshot of retained SQL outbox events into a fresh replay topic. Delivery markers and live namespace remain untouched. Snapshot replay is not a subscription to future changes. Kafka retains seven days; outbox/inbox/projections currently retain all records and need a coordinated long-term policy. See KAFKA.md for executable commands and limitations.

## Executed verification

| Verification                                         | Observed result                                                                                                                                                                                                                                                             |
| ---------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Production API/dashboard/event images and C++ engine | Built successfully; all migrations001–005 applied                                                                                                                                                                                                                           |
| Normal C++ GoogleTest/CTest                          | Three concurrency/queue/shutdown tests passed                                                                                                                                                                                                                               |
| Node contract suite                                  | Three tests passed: DAGs, handler validation and canonicalization                                                                                                                                                                                                           |
| Full-feature PostgreSQL/Redis/Kafka integration      | 13/13 passed, including real C++ results, tenant isolation, retry/cancellation, stale acceptance race, DAG ordering, two workers/two schedulers and killed-worker recovery                                                                                                  |
| Redis transport/shared quota/OOM                     | Passed exact concurrent quota across replicas, stale/foreign cache, fail-closed outage, reconnect and noeviction memory exhaustion                                                                                                                                          |
| Real Kafka pipeline                                  | Passed rollback, ack-loss duplicate publication, producer failure/restart, idempotent effects, unique out-of-order records, invalid schemas, consumer stop/restart and both Kafka/retained-SQL replay                                                                       |
| Complete persistent Compose stack faults             | Publisher stopped while a job completed and5 events remained pending; restart drained it. Broker stop preserved execution/outbox; restart reached zero lag. Redis stop rejected admission while an acknowledged job survived; reconnect restored API. Diamond DAG completed |
| Browser verification                                 | Authenticated live Metrics page displayed actual Redis state/cache counters, project outbox, zero observed Kafka lag, pipeline availability, dispatches, queue wait, execution p95 and analytics counts                                                                     |
| TypeScript and production bundle                     | Typecheck and build passed                                                                                                                                                                                                                                                  |
| Production dependency audit                          | 0 low/moderate/high/critical vulnerabilities reported by npm audit --omit=dev at audit time                                                                                                                                                                                 |
| AddressSanitizer                                     | C++ concurrency and all11 core PostgreSQL/process checks passed with a non-PIE debug build                                                                                                                                                                                  |
| ThreadSanitizer on local WSL/Docker                  | Unsupported: even an empty program exits with unexpected memory mapping; no local race-free runtime claim                                                                                                                                                                   |
| GitHub Actions                                       | Reliability, Redis and Kafka milestone workflows completed successfully; final follow-up CI is checked separately after push                                                                                                                                                |

Default PIE ASan startup was intermittent on this host: an initial CTest run failed, and a later process run passed9/11 with children failing before ASan initialization completed. Repeating an empty main reproduced DEADLYSIGNAL, independently of TaskForge. The non-PIE ASan build then passed11/11 core checks; flags and commands are in TESTING.md. These earlier failures are not counted as passes. No production compiler flags were weakened.

The public test Compose project is disposable, uses a test-named database, publishes no infrastructure ports and truncates fixtures. It does not use the main application's credentials/data. CI installs test dependencies, runs the suites, and preserves failure status; only the exact unsupported TSAN mapping condition is annotated as unsupported. A green build does not certify exhaustive correctness or infrastructure security.

## Actual measurements

Raw observations: benchmarks/results/2026-10-21-audit.json. Executable: benchmarks/audit.ts. Full-stack fault outcomes: benchmarks/results/2026-10-21-stack-faults.json and benchmarks/stack-faults.mjs. The older four JSON files predate Redis/Kafka and are historical samples.

Environment: i7-13700H developer laptop, Docker Desktop Linux/WSL2 on Windows,20 visible CPUs and7.62GiB visible RAM, Node22.23.4, GCC12 C++20 Debug, PostgreSQL17, Redis7.4 and Kafka3.9.1. Four lanes/worker, queue limit8, eight concurrent HTTP submitters,80 synthetic sleep10ms jobs per configuration.

| Workers/schedulers | Submit jobs/s | Observed completions/s | Completion p95ms | Queue-wait p95ms |
| ------------------ | ------------- | ---------------------- | ---------------- | ---------------- |
| 1/1                | 44.45         | 20.15                  | 2165.11          | 2117.18          |
| 2/1                | 72.13         | 27.77                  | 1750.65          | 1717.38          |
| 1/2                | 69.19         | 18.70                  | 1673.13          | 1592.24          |
| 2/2                | 75.82         | 34.82                  | 1306.33          | 1283.16          |

No extra attempts or observed deadlocks in these batches. Sampled peak lock waiters were0/1/0/0. Sampling after submission can miss contention. One extra scheduler did not improve the one-worker sample; linear scaling is not established. Polling and transactional coordination dominate these tiny handlers. No engine optimization was justified from these single-run observations.

A49,921-byte job detail measured100 warm hits then100 forced misses: mean12.21ms vs19.07ms; p95 20.06ms vs29.34ms. Key deletion is outside timing and both paths retain auth/shared admission. This supports a workload-specific benefit, not a universal percentage speedup. Cache invalidation/version checks and Redis traffic have costs, particularly for frequently renewing jobs.

The publisher drained1,619 events in17 batches in0.784s, or2,064.09 events/s including SQL claims/ack writes. The consumer reached zero lag in5.480s including group start, or295.45 observed events/s. One local broker, replication1 and three partitions do not establish production broker capacity.

One killed-worker recovery sample took15.66s to durable completion and used three attempts for a2s handler. A recent dead heartbeat permitted another wasted assignment. Lease duration alone is not a recovery bound. Raw JSON includes all p50/p95/p99 distributions and caveats; no statistical confidence or sustained capacity is claimed.

## Security, reliability and production assessment

Parameterized SQL, project-scoped HTTP access, hashed random tokens, payload/graph/active-job/queue limits, fixed handler registry and separate API/scheduler/worker/publisher/consumer database credentials remain in place. Publisher and consumer cannot update jobs; regressions verify permission denial. Application containers run non-root with read-only filesystems/capability restrictions. Only loopback HTTP is published; generated secrets, local logs and test artifacts remain ignored. The public tests contain disposable credentials only.

Execution and outbox delivery are at least once. External effects must be idempotent using stable job IDs, remote uniqueness or business ledgers. Attempt fencing protects durable result ownership, not external side effects. Built-in handlers cooperate with cancellation; arbitrary host commands remain prohibited. SQL/client socket deadlines bound common stalls, while DNS resolution/custom uncooperative handlers remain outside that guarantee.

This is a demonstrable, tested portfolio platform suitable for controlled local evaluation. It is not an unattended production service. Remaining work includes PostgreSQL replication/failover and restore drills; replicated Kafka with TLS/SASL/ACLs; coordinated event/inbox/analytics retention; disk monitoring; per-user authorization; trusted proxy/TLS configuration; large-history query/capacity studies; and stronger handler/resource isolation if adding custom workloads. Kafka's internal PLAINTEXT assumes trusted hosts, and one broker cannot tolerate disk/broker loss. Historical projections can retain records beyond source-job retention. Priority scheduling can starve lower priorities and is not weighted per-tenant fairness.

The interview-ready value is the executable explanation of invariants and failure windows, not a claim of exactly once, global scale or production certification. INTERVIEW_GUIDE.md, LEARNING_GUIDE.md and RESUME_GUIDE.md refer to the actual implementation and qualified measurements.

Fresh-storage verification: the final public Docker suite rebuilt its source image, applied all five migrations to an empty test database, and passed3/3 contract tests,3/3 C++ tests and13/13 integration tests. The tests service exited0. Kafka exit143 occurred during Compose's intentional shutdown after the successful tests; it was not the test exit status.
