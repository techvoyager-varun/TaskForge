# Operations

## Lifecycle

`node scripts/setup-env.mjs` preserves existing secrets and appends missing independent database/Redis credentials in ignored `.env`. Compose starts PostgreSQL, waits for its health check, applies migrations and role provisioning, then starts the API, schedulers, workers and independent Kafka publisher/consumer. Redis and Kafka have separate health checks; Kafka data persists in kafka-data. Images compile source from the tracked repository and do not copy `.local/`, `.env`, build output or Git metadata.

```powershell
docker compose up -d --build --scale worker=2 --scale scheduler=2
docker compose run --rm admin "My project"
docker compose ps
docker compose logs --tail 30 migrate api scheduler worker
```

The owner CLI creates a default queue and prints a token once. Avoid redirecting its output into tracked files. Tokens and `.env` are credentials. Back them up privately if needed. Database persistence is in the named `postgres-data` volume, not in a worker process. Keep `.env` paired with that volume.

## Role boundaries

The owner login is used only by migration/admin containers. The API, scheduler and worker have independent login credentials and non-login capability roles. Worker logins cannot directly select jobs, scheduler logins cannot read projects, and API logins cannot execute dispatch. API reads are project-filtered; this version does not use row-level security.

The API container and C++ containers run as non-root users with read-only root filesystems, dropped capabilities and no-new-privileges. The database port is not published. Only loopback HTTP is exposed by default. Redis now shares admission limits across API replicas. Remote deployment still requires a deliberate TLS/reverse-proxy setup, trusted proxy/IP configuration and Kafka authentication/ACLs.

## Schema upgrades

```powershell
docker compose run --rm migrate
```

Migration names and SHA-256 checksums are recorded atomically with schema updates under a database advisory lock. Applied files must not be edited; add a new numbered migration. Bootstrap is safe to rerun with the same environment. Password rotation needs coordinated changes to `.env`, provisioning and service recreation; replacing `.env` alone does not replace an existing PostgreSQL volume's owner password.

## Rotate a project token

```powershell
docker compose run --rm --entrypoint node admin dist/rotate-token.mjs '<project UUID>'
```

Keep the new output private. New HTTP requests using the old token fail immediately. Existing live streams revalidate at most every 30 seconds and close within that interval; they also reconnect/re-authenticate every 15 minutes. Disconnect and reconnect the dashboard with the new token.

## Retention

The default is to retain all history. The administrator tool only selects terminal standalone jobs older than 30 days and locks at most 500 per transaction. Its default action is a dry run:

```powershell
docker compose run --rm --entrypoint node admin dist/retention.mjs
```

Add `--apply` only when you intend to delete those eligible job records and their attempts/logs/audit/dead-letter entries. Set `RETENTION_DAYS` with `docker compose run -e RETENTION_DAYS=90 ...` to change the window (7..3650 days). Repeat batches until no eligible records remain. Workflow executions are retained to preserve graph lineage. Active work is never selected. Deleting a job also expires its submission idempotency key, so use a retention window longer than clients' replay window.

## Capacity and recovery

Queue concurrency limits active leases, not historical worker count. Each worker defaults to four lanes; set WORKER_CONCURRENCY to 1..256 in Compose and recreate workers to change it. Old worker identities remain for attempt history; availability depends on heartbeat age, not row existence. The dashboard only shows workers associated with the authenticated project's attempts.

Leases expire after six seconds unless renewed; each attempt has a separate fixed deadline. The scheduler checks for recovery every 100 ms, but database load, backoff, stale availability and queue occupancy can extend total recovery time. Multiple schedulers are safe and recommended for the failure demonstration. No automatic PostgreSQL failover is provided.

## Monitoring

`/health/live` checks HTTP; `/health/ready` checks the API database connection. Worker/scheduler operational events are structured JSON on stdout. Job assignment, handler start and outcome are stored in execution_logs. `/api/v1/metrics` requires a project token and emits durable state counts and completion latency percentiles in Prometheus text format. Configure a Prometheus scraper's authorization bearer file privately if needed; there is no unauthenticated metrics bypass or bundled scraper credential.

The SSE dashboard reads actual two-second database snapshots. Database aggregation costs grow with history. Use retention and measure query load before increasing dashboard connections. Slow streams and streams exceeding the per-project/process cap are closed/rejected.

## Backups and shutdown

Use PostgreSQL's normal pg_dump/restore procedure for backups and verify restores in an isolated environment. No backup automation is shipped. `docker compose stop` preserves the named volume. Worker SIGTERM marks the worker draining, requests cooperative handler stop, joins threads, and leaves unfinished leases recoverable. A forced kill loses in-memory execution but not acknowledged jobs.

## Redis, events and replay

Read [Redis](REDIS.md) and [Kafka](KAFKA.md) for failure behavior and executable replay commands. The Metrics page and authenticated /api/v1/infrastructure expose current Redis status, per-process cache counters, project outbox depth/age, deployment consumer lag and project execution/queue-wait p95. Unavailable lag is not rendered as zero. No Redis/Kafka host ports are published. Redis noeviction preserves active rate counters under memory pressure; API admission fails closed on Redis outage/OOM while existing C++ execution continues.

The outbox, inbox, analytics and failure-marker tables currently retain all records independently of terminal-job retention. Monitor their disk usage. Kafka retains seven days and uses a single broker: replay older retained SQL events with the supplied snapshot tool. Back up PostgreSQL and Kafka volumes separately; do not infer end-to-end recovery from a single backup.

API SIGTERM ends SSE streams before closing HTTP/SQL connections. C++ uses bounded client I/O in addition to server timeouts. DNS resolution and arbitrary custom handlers are outside that deadline guarantee.
