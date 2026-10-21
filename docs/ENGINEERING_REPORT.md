# TaskForge engineering report

TaskForge now implements a working local distributed execution platform across an Express API, PostgreSQL, separate C++20 scheduler/worker executables and a React dashboard. Docker Compose starts the full stack with independent database credentials and persistent storage. It is a functional portfolio implementation with explicit guarantees and boundaries.

## Implemented and exercised

Job submission, project-scoped idempotency, payload validation, priority, delayed eligibility, bounded worker pools, queue/worker capacity, heartbeats, renewable leases, increasing attempt fences, durable results/history/logs, retries with jitter, per-attempt timeouts, cancellation, dead-lettering and quota-safe manual retry are implemented. The scheduler owns dispatch timing and recovery; PostgreSQL transactions decide whether a state change may commit.

Workflow submission validates graph structure and handler inputs, inserts the graph atomically and starts explicitly. Independent nodes execute concurrently. Children wait for all parents; exhausted/cancelled predecessors propagate cancellation. Retrying creates a new execution linked to its original history. The dashboard exposes real job details, attempts, graph states, queue depth, workers and completion metrics through authenticated HTTP/SSE.

Security includes hashed random project tokens, parameterized values, project-scoped HTTP queries, request limits, bounded payloads and graphs, fixed handler allowlists, separate service capability roles, non-root application containers, internal PostgreSQL networking and ignored generated credentials. The API's project/queue submission quotas coordinate through database advisory locks. The public source contains no private tokens or local test suite.

## Evidence

The final local suite passed real worker-crash recovery, competing schedulers, capacity checks, retry/timeout/cancellation behavior, scheduler termination, PostgreSQL restart persistence, HTTP tenant isolation/idempotency, DAG ordering/failure propagation, microsecond cursor pagination, live-stream limits and revocation, permission checks and retention behavior. Three GoogleTest pool checks passed. A real AddressSanitizer worker run completed and shut down without a report on the exercised path. An independent review found three concurrency/quota defects, all reproduced and fixed in dedicated commits before a focused re-review found no remaining issue in those corrections.

Strict TypeScript checks, production builds and C++ warning-as-error builds passed. GitHub build/audit/sanitizer-compilation jobs passed for the published code. The final Compose stack has a healthy API and database with two schedulers and two four-lane workers. See verification.md for precise commands, scope and limits.

## Observed performance

Single local baseline samples of 80 jobs observed 27.85 and 46.36 HTTP-completions/s for one- and two-worker synthetic no-ops. Timed 25 ms sleep work observed 23.35/s; summing 10,000 numbers observed 28.32/s, with a checked result. Raw outputs include submission throughput, latency percentiles and host specifications. These were short, non-isolated measurements during implementation; they do not establish sustained capacity, a general scaling ratio or production performance.

## Boundaries and unfinished advanced scope

- Project-token authorization is implemented; individual user accounts, role hierarchies and tenant-level database RLS are not.
- Execution is at least once. External side effects require application idempotency; exactly-once effects are not promised.
- Built-in handlers cooperate with stop/renewal. Custom sandboxed code execution, automatic CPU/memory-based slot adaptation and hard process isolation per job are not implemented.
- Queue round-robin and per-project active quotas exist. Weighted tenant fairness and low-priority starvation prevention do not.
- Workflows order nodes but do not automatically bind predecessor outputs into child payloads. The graph is immutable for each execution.
- Redis/Kafka, gRPC and OpenTelemetry are deliberately deferred. PostgreSQL remains a single availability dependency; replication, automatic failover and backup automation are not shipped.
- Prometheus-format metrics and live snapshots exist. A preconfigured scraper server, distributed traces and high-volume incremental metric aggregates are not bundled.
- Retention is a bounded administrator action for terminal standalone jobs. Workflow archival remains future work.
- API IP-rate and live-stream limits are per process; shared limiting must precede public multi-replica operation.
- The ThreadSanitizer runtime was blocked by a Docker memory-mapping failure; no runtime race-detector success is claimed.
- Tests remain in ignored local files at the owner's request. CI validates builds and production dependency security, not the private fault suite.
- Longer repeatable benchmarks, database query-plan profiling, arbitrary network partitions, TLS deployment and operational failover remain future engineering work.

The first continuation should improve independently reproducible validation if the owner later permits publishing tests, then address measured database/observability bottlenecks and deployment needs. Adding infrastructure without a demonstrated responsibility would weaken the current explainable design.
