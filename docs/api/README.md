# HTTP API

Base path: `/api/v1`. Every endpoint below requires `Authorization: Bearer tf_<64 hex characters>`. Project creation and token rotation are trusted administrator CLI operations. Authentication compares a SHA-256 token hash; the database does not store raw project tokens. User accounts and individual role management are outside this version.

Success responses use `{ "data": ... }`. Lists of jobs/workflows/executions include `next_cursor`; use it as the next request's `cursor`. The maximum page size is 100. Job logs use an integer `after` cursor and maximum 200 entries. Worker lists are bounded to the most recent 100 project-associated workers. Queue lists are bounded by the 32-queue project quota.

| Method     | Path                      | Behavior                                                                          |
| ---------- | ------------------------- | --------------------------------------------------------------------------------- |
| GET        | /project                  | Current project ID and name                                                       |
| POST / GET | /queues                   | Create or list project queues                                                     |
| GET        | /queues/:id/stats         | Durable counts by job state                                                       |
| POST / GET | /jobs                     | Submit or list jobs                                                               |
| GET        | /jobs/:id                 | Job, payload, result and attempt history                                          |
| POST       | /jobs/:id/cancel          | Cancel queued work or request running cancellation                                |
| POST       | /jobs/:id/retry           | Grant three additional attempts to a terminal standalone job                      |
| GET        | /jobs/:id/logs            | Durable execution events                                                          |
| POST / GET | /workflows                | Create a validated draft or list executions                                       |
| GET        | /workflows/:id            | Execution with nodes and edges                                                    |
| POST       | /workflows/:id/run        | Start a draft                                                                     |
| POST       | /workflows/:id/cancel     | Cancel the execution's jobs                                                       |
| POST       | /workflows/:id/retry      | Clone a terminal execution to a fresh draft with linked lineage                   |
| GET        | /workflows/:id/executions | Paginated workflow execution lineage                                              |
| GET        | /workflows/:id/attempts   | Up to 500 node-attempt records                                                    |
| GET        | /workers and /workers/:id | Heartbeats/capacity for workers associated with the project                       |
| GET        | /overview                 | Counts, last-24-hour completion percentiles and last-30-minute completion buckets |
| GET        | /metrics                  | Authenticated Prometheus text format                                              |
| GET        | /events                   | Actual overview snapshots through Server-Sent Events                              |

Outside this prefix, `/health/live` checks the HTTP process and `/health/ready` checks its database connection. They do not require authentication.

## Inputs

Job body: `queue_id` UUID, `type`, handler-specific `payload`, optional `priority` (-100..100), `max_attempts` (1..20), `timeout_ms` (100..3,600,000), and `available_at` (ISO timestamp). Defaults: priority 0, three attempts, 30-second deadline, immediate availability. Unknown top-level properties are rejected. Body limit: 128 KiB.

- `noop`: `{}`
- `echo`: a JSON object, returned as the result
- `sum`: `{ "numbers": [2,3,7] }`, up to 10,000 finite numbers in [-1e100,1e100]
- `sleep`: `{ "duration_ms": 2000 }`, up to one hour
- `fail`: `{ "message": "demonstration", "fail_until_attempt": 1 }`; attempts at or below the threshold fail, later attempts return recovered=true

Workflows contain 1..200 nodes and up to 2,000 directed edges. Each node's handler contract is validated before the creation transaction. Duplicate keys, duplicate edges, unknown references, self-cycles and multi-node cycles are rejected. Graph structure is immutable after creation. A workflow currently represents one execution; retries create related execution records.

## Errors and limits

Errors use an `error` object with `code` and `message`; validation errors also include `details`. Most errors carry a `request_id` also returned through `X-Request-Id`. Unknown project resources return 404, preventing disclosure of another project's IDs. Malformed inputs return 400; unauthorized requests 401; conflicting idempotency keys/state 409; rate/quota limits 429; unconfirmed database operations 503.

Limits are 300 requests/minute per client IP, 10,000 active jobs/project, 32 queues/project, and ten simultaneous live streams/project/process. Redis atomically shares the IP limit across configured API replicas. The stream cap remains per project/process. Redis outage or memory exhaustion fails admission with 503; existing acknowledged jobs remain durable. Queue/worker capacity and project submission quotas remain database-coordinated. Deploy behind TLS before using remote clients.

SSE uses `event: snapshot` and JSON data every two seconds. The browser supplies its bearer header using fetch streaming. Streams close after 15 minutes and reauthenticate on reconnect; token validity is also checked every 30 seconds. Slow clients with more than 64 KiB of queued stream output are disconnected. Token rotation invalidates subsequent HTTP requests immediately and open streams within a revalidation interval.

## Idempotency and cursors

`Idempotency-Key` is optional, project-scoped, case-sensitive and limited to 128 visible ASCII characters. The request fingerprint sorts JSON object keys recursively, normalizes defaults through validation, and includes queue and scheduling options. Keys are not reusable for different requests. Retention deletion ends the deduplication window.

Opaque pagination cursors preserve PostgreSQL timestamp precision and sort `(created_at,id)` descending. They are positioning tokens, not a snapshot isolation guarantee: concurrent inserts can appear before the current page, and changing filters starts a new traversal. Client-provided cursors are validated and all SQL values are parameterized.

## Cache and infrastructure measurements

GET /jobs/:id validates project ownership and current state_version in PostgreSQL before a version-qualified Redis read. X-TaskForge-Cache reports hit or miss when configured; active entries have TTL5s and terminal entries TTL60s. Cache availability never replaces PostgreSQL authorization or state validation.

GET /infrastructure returns Redis client readiness and process-local hit/miss/error counters, this project's outbox pending/delivered/age/retry measurements and live analytics counts, shared-deployment pipeline heartbeat/availability/lag, and project dispatch/queue-wait/execution/failure measurements. Missing or unavailable lag is null. No event payload/result/token material is exposed in this endpoint. GET /metrics includes Redis availability/cache counters, project outbox pending and observed pipeline availability/lag in Prometheus format. Project bearer authentication applies to both.
