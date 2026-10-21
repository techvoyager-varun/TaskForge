# Audit and event-integration plan — 21 October 2026

Scope: improve the existing implementation in place. The new request explicitly supersedes the previous local-only test policy. Tests will now be public and reproducible; credentials and transient artifacts remain private. Continue verified milestone commits and pushes.

## Audit baseline

Reviewed C++ scheduler, worker, libpq wrapper, pool and handlers; all applied migrations and current procedure overrides; API auth/quotas/idempotency/pagination/workflows/SSE; React request handling and graphs; Compose, images, CI, scripts, benchmarks and documentation. Independent coordination review found no new confirmed critical issue in the existing ownership protocol. Confirmed input defect: an empty Idempotency-Key bypasses validation and becomes a database error instead of HTTP 400. Reproduce before correction. Investigate bounded database I/O under a real blackhole before making a shutdown guarantee.

## Commit-sized sequence

1. Publish portable unit/SQL/HTTP/process checks and CTest/GoogleTest integration. Reproduce and correct confirmed input/connection issues. Run on an isolated database from a clean source image; execute public tests in CI.
2. Add Redis shared rate limiting with atomic Lua and explicit fail-closed admission during outage. Cache job detail snapshots by PostgreSQL state_version; query current project-owned version before every cache read. TTLs bound retention, versioned keys prevent stale writes from becoming current. Redis errors bypass caching, never replace durable state. Measure counters and test concurrency, stale cache and outage/reconnect.
3. Add schema-versioned lifecycle events through PostgreSQL triggers and an outbox in the state transaction. A restartable Kafka publisher marks delivered only after broker acknowledgement; duplicate publication is expected across crashes. Use per-aggregate keys and a fenced batch claim/ack protocol.
4. Add an idempotent Kafka consumer with a durable deduplication ledger, monotonic job/workflow projections, and persisted consumer offsets/health. Commit projection and inbox atomically before acknowledging Kafka offsets. Replay builds a new projection namespace; consumers never write authoritative jobs.
5. Integrate private Redis/Kafka networking and least-privilege event roles in Compose. Add real monitoring, outbox backlog, observed consumer lag and availability. Benchmark current stack and cache/event paths; publish raw measured observations only.
6. Exercise full-stack outages, restarts, worker crashes, replay/duplicate delivery, tenant access and sanitizer checks. Update setup/API/architecture/interview/learning/resume guides and FINAL_ENGINEERING_REPORT.md. Review, push and confirm CI.

## Contract and failure rules

PostgreSQL and the C++ scheduler keep ownership of durable scheduling. Redis does not authorize a cached record without a current project-scoped PostgreSQL version check. Shared rate-limit loss returns 503 before new admission; existing workers continue. Kafka failure only accumulates the durable outbox. Publisher delivery and job execution are at least once; consumer deduplication protects derived database effects. Events exclude job payloads, results, token hashes and raw errors. Replay is namespaced to avoid resetting live consumer state.

Tests must cover fresh migrations/role grants, conflicting claims, acceptance snapshots, stale leases, priorities, retries/cancellation/DAGs, authorization, Redis atomic quota/outage/stale data, outbox rollback/ack loss, duplicate/out-of-order consumer effects and replay. Unsupported sanitizer runtimes must be reported explicitly rather than silently counted as passes.
