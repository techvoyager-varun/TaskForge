# Failure model

| Event | Durable behavior | Verification / limit |
| --- | --- | --- |
| Worker killed during a handler | Current lease expires; attempt records failure; retry receives a larger fence | Real worker container killed; job completed on attempt 3 |
| Scheduler terminated | Another scheduler continues; otherwise durable work waits | One of two scheduler processes killed; subsequent echo completed |
| Two schedulers compete | Worker/queue row locks and conditional assignment serialize grants | 30 concurrent sleep jobs, one attempt each; observed queue occupancy never exceeded 2 |
| Duplicate submission | Same project/key/request returns original job; conflicting request receives 409 | Real HTTP checks, including changed payload |
| PostgreSQL restarted | No completion is acknowledged while disconnected; clients reconnect; pending jobs survive | Five persisted jobs completed after database restart |
| Redis unavailable | No effect on initial execution path | Redis is not a runtime dependency |
| Worker heartbeat expires | Scheduler stops assigning to that worker; existing leases remain independently fenced | Crash test exercised the availability window |
| Long handler | Cooperative checkpoints renew the lease until the per-attempt deadline | Sleep handler exercised renewal, timeout and cancellation |
| Completion arrives after expiry | Rejected even if reassignment has not happened yet | Direct database check |
| Completion uses an old attempt | Rejected without overwriting the new attempt | Direct database check after retry |
| Two parents complete | Maintenance makes a child READY only after both complete | Diamond graph checked against actual assignment/parent completion timestamps |
| Cancellation races completion | A locked CANCEL_REQUESTED job resolves to CANCELLED | Database race-state check and real running cancellation |
| Retry budget exhausted | Attempt outcome preserved; job becomes DEAD_LETTERED with durable reason | Failing handler exhausted exactly two attempts |
| Workflow predecessor fails | Pending descendants are cancelled transitively; execution becomes FAILED | Four-node graph failure propagation check |
| Network partition | Work may have effects but cannot confirm completion; lease recovery permits another execution | Database restart tested; arbitrary TCP blackhole behavior was not fault-injected |
| Machine or volume loss | Availability/durability depends on PostgreSQL storage and backups | No replication or disaster-recovery claim |

Acknowledged submission means the database commit succeeded before the HTTP success response. A lost HTTP response after commit is resolved by retrying with the same idempotency key. A worker never treats an unconfirmed completion as durable success. Scheduler and worker database errors cause bounded-delay reconnection attempts.

The deadline starts at assignment, including the short interval before a worker lane accepts it. A crashed worker can consume multiple assignments while its last heartbeat still appears live. Six-second leases and a ten-second availability window therefore bound coordination intervals rather than guaranteeing a six-second end-to-end recovery time.

Automatic retry delay uses uniform jitter between half and all of the exponentially increasing base delay (500 ms initially, capped at 300,000 ms). Manual retry grants three additional assignments and preserves the attempt counter. Workflow retry creates a separate execution. All timing uses PostgreSQL for lease decisions; worker monotonic clocks only decide when to request renewal.

Signals stop new worker activity, mark the worker draining where PostgreSQL is available, request cooperative thread shutdown, and join all threads. An unfinished assignment is left for lease recovery. Shutdown does not falsely mark work complete.
