# Durable execution model

## Boundaries

The Express API authenticates project tokens, validates payloads, and writes job requests. A separate C++20 scheduler selects available workers and asks PostgreSQL to grant leases. C++ workers execute a fixed registry of handlers through a bounded thread pool and persist results with a fenced compare-and-update. The React dashboard reads real API data. Redis is deferred until notifications demonstrably need it; durable coordination never depends on it.

The first increment uses PostgreSQL/libpq directly between trusted services. This avoids an additional RPC failure boundary. Database procedures own transactional invariants; only the scheduler calls dispatch. Dedicated database credentials grant the API submission/inspection functions and workers only execution functions. Network access to PostgreSQL is internal in Compose.

## Data model

Projects have hashed API tokens and contain queues. Jobs have a queue, type, validated JSON payload, priority, availability time, maximum attempts, timeout, current attempt number, owner and lease deadline. Attempts preserve every assignment and outcome. Workflow nodes are jobs; edges reference jobs in the same workflow. Logs and dead-letter records are durable. Project-scoped idempotency keys compare request contents and reject reuse for a different request.

## State machine

PENDING -> READY -> RUNNING -> COMPLETED.
RUNNING -> RETRY_WAIT -> READY, or DEAD_LETTERED when attempts are exhausted.
RUNNING -> CANCEL_REQUESTED -> CANCELLED.
PENDING, READY and RETRY_WAIT can be cancelled directly. Terminal retry creates another allowed execution budget without resetting the fencing counter. Attempt outcomes distinguish FAILED, TIMED_OUT, CANCELLED and COMPLETED. Invalid state changes are rejected by a database trigger.

## Lease protocol

The scheduler locks a live worker and queue row before checking capacity. It then selects an eligible job using FOR UPDATE SKIP LOCKED and atomically increments its attempt number, inserts an attempt and sets RUNNING with a lease. Queue locks serialize capacity checks; worker locks serialize worker capacity checks. Lock acquisition order is consistent (worker, queue, job). Maintenance takes job locks only and never takes capacity locks afterward.

Workers accept each assignment with a status-qualified attempt update and return work only when that update succeeds, even if their joined SELECT snapshot was stale. Workflow cancellation takes the maintenance advisory lock before row locks, preventing lock-order inversions with maintenance. Manual retries take the same project quota lock as submissions. Heartbeats renew only the current unexpired lease and never extend past the execution deadline. Completion succeeds only for the current worker, attempt number, state and unexpired lease. Late completions are rejected even before a new assignment exists. Expired leases are recovered to retry wait or dead-letter by the scheduler. Duplicate effects remain possible when work finishes before a completion is durably recorded: external handlers must use an application idempotency key.

## Workflows

Submission validates references and rejects duplicate edges, self-dependencies and cycles using Kahn's algorithm in O(V + E) time and space. Workflow creation is one transaction. A workflow starts explicitly; roots become eligible first. Maintenance promotes a dependent only when every predecessor is COMPLETED. A job row can change to READY only once per waiting phase. Failed, dead-lettered or cancelled predecessors cancel downstream nodes. Workflow state is derived after node transitions. Independent nodes can execute concurrently.

## Failures and security

Database failure means fail closed: no acknowledgement before durable insert, no success claim without durable completion. Restart preserves requests, leases and workflow state. A crash after claiming wastes at most a lease interval. Redis failure has no effect because Redis is not in the initial execution path. Cancellation and timeout are cooperative for built-in handlers; arbitrary shell/code submission is forbidden. Credentials come from environment variables; API tokens are hashed; SQL uses bound parameters; API reads and writes are project scoped.

## Alternatives

A Node-only queue would simplify deployment but would bypass the C++ scheduling requirement. gRPC between every service would add transport complexity before it improves this vertical slice. Direct PostgreSQL coordination is chosen for auditable transactional correctness; separate executables preserve the scheduler/worker boundaries.

## Verification

Local checks must exercise real PostgreSQL and real C++ processes: duplicate submissions, competing schedulers, worker capacity, stale completions, crash recovery, retry exhaustion, cancellation, DAG ordering and cross-project access. Only observed results will be reported. Tests are local and ignored by Git at the owner's request.
