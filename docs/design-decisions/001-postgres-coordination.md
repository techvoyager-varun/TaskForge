# ADR 001: PostgreSQL owns durable coordination

Status: accepted and implemented.

The API, scheduler and workers are separate processes. Sharing durable procedures through libpq/pg keeps the first implementation's consistency boundary inside one database transaction. gRPC would require another request/acknowledgement protocol without removing the database transaction. Redis notification loss would still require reconciliation, so polling PostgreSQL is the initial design.

The scheduler controls when dispatch and maintenance are invoked. The database controls whether an assignment or completion may commit. Worker and queue row locks serialize capacity decisions; SKIP LOCKED lets competing schedulers move past occupied rows. Queue selection uses oldest last-dispatched time, giving queues a round-robin opportunity. Job selection within a queue is priority first and FIFO by availability/creation time; this can starve low-priority work under sustained high-priority input. A tenant with multiple queues gets more opportunities than one with a single queue; weighted tenant fairness is not implemented.

Increasing attempt numbers are fencing tokens. Completion requires the matching worker and current attempt plus an unexpired lease. A timeout or lost lease records the attempt outcome, schedules a retry, or exhausts the job. Lease duration is six seconds; worker availability expires after ten seconds. Following a crash, the scheduler can grant another attempt to a recently dead worker during this availability window. That assignment still consumes an attempt and is recovered again. These are bounded leases, not a claim of instantaneous failure detection.

At-least-once execution follows from the uncertain interval between effects and completion acknowledgement. Exactly-once effects require an external operation with its own deduplication/transaction mechanism. TaskForge's request key prevents duplicate submissions, not repeated effects during retries.

All service logins are non-superusers. The scheduler can inspect workers and execute dispatch/maintenance procedures; workers can execute registration, acceptance, renewal and completion procedures but cannot directly read job tables. API access is constrained to submission/inspection and selected project-scoped operations. Procedures use a fixed search_path, and public function execution/schema creation are revoked. These roles are a service boundary; tenant isolation in the HTTP API is enforced by project-scoped queries rather than database row-level security.

Consequences: coordination is auditable and durable, but PostgreSQL is a single availability dependency. Polling imposes a latency floor and database load. Backpressure is explicit queue/worker capacity plus project quotas. A notification accelerator can be added later without changing the source of truth.

Reference: [PostgreSQL SELECT locking clauses](https://www.postgresql.org/docs/17/sql-select.html).
