# Learn TaskForge progressively

## 1. Run the system before reading implementation details

Follow the README Compose setup and create a project. Submit an echo job from the dashboard. Inspect its payload, result, attempt and execution logs. Notice that refreshing the page or restarting workers does not remove the job: persistence is in PostgreSQL.

Read `packages/contracts/src/index.ts` to see the input shapes. Change handler selection in the submission dialog and observe different validated payloads. Try a sum payload with a string instead of a number and observe rejection before it becomes durable work.

## 2. Trace the HTTP transaction

Read `apps/api/src/app.ts`, then `jobs.ts` and `database.ts`. Authentication derives a project from a token hash. Every resource query uses that project ID. Submission takes a project advisory lock, checks the queue and active quota, then inserts a job and audit record inside one transaction. No success response precedes COMMIT.

Submit twice with the same idempotency key and body using the README example. Both responses identify the same job. Change the payload but retain the key and observe 409. Canonical JSON sorts object keys recursively so property ordering alone does not change the fingerprint.

## 3. Read the durable state machine

Open migration 001. Start with the jobs and job_attempts tables, foreign keys, partial indexes and validate_job_transition trigger. A job tracks the current state; attempts preserve each ownership grant and outcome. FAILED and TIMED_OUT are attempt outcomes; the job retries or eventually becomes DEAD_LETTERED.

Next read migrations 002 and 003. Migrations are an append-only history, so the latest implementation of next_assignment and retry_job is in 003. Do not change an applied migration: the runner checks its checksum.

## 4. Understand assignment before execution

Read `services/scheduler/main.cpp` and `dispatch_job`. Each scheduler repeatedly runs maintenance and inspects live workers. Database locking makes the choice safe across processes. Queue and worker row locks serialize their capacity checks. The ready index supports selection by queue, priority and time.

Then read latest `next_assignment`. Selection and acceptance are separate operations. The joined ASSIGNED status may come from an older SELECT snapshot, so the actual attempt update must still require ASSIGNED and must succeed before returning a job. This is a concrete example of why a lock on one table is not a fresh-state check on another table.

## 5. Study C++ ownership and shutdown

`database.hpp` wraps PGconn/PGresult in unique_ptr deleters, making exception paths free resources. Each worker lane owns its own connection. `thread_pool.hpp` owns every thread and synchronizes its pending deque. `runtime.hpp` provides stop-aware waits and serialized structured output.

Read the worker's main function, lane loop and execute function. On shutdown, no detached thread outlives pool state. On a database error, a lane reconnects after waiting. It never logs a durable completion unless finish_job confirms it.

## 6. Learn leases, fences and retries through a failure

Use the interview guide's single-worker crash demo. The original worker's last heartbeat is initially still fresh. Its job lease expires, the scheduler records failure, and a new attempt eventually runs. The attempt counter increases even for assignments lost to a crash.

Read ExecutionContext and renew_lease. A handler must cooperate by calling checkpoints; a lease is not a mechanism for killing arbitrary operating-system work. Read finish_job and note that both the fence and lease expiry are checked before persisting results. Explain why an external effect could still repeat.

## 7. Add a handler safely

A new handler requires a contract in packages/contracts, an allowlisted function in handlers.hpp, and an allowed database type in a new migration. Keep its payload bounded, avoid shell execution, and call checkpoints during long work. Add a local ignored behavioral check for payload rejection, correct result and cancellation/timeout behavior. Compile and run a real persisted job before committing the feature. Do not add handler-specific mechanisms to the scheduler.

## 8. Understand workflows

Start with topologicalOrder, which uses indegrees and an adjacency list. Draw a diamond: A precedes B and C; both precede D. Follow how the API transaction inserts nodes and edges, how starting changes the workflow state, and how maintenance promotes eligible nodes.

Try a cycle in the workflow editor and confirm that creation fails. Run a valid graph and inspect parent completion times versus child assignment times. Try a failing root: its descendants cancel after its retries are exhausted. Retrying the workflow produces another execution, not rewritten history. Outputs are currently inspected separately rather than automatically bound to downstream inputs.

## 9. Connect monitoring to real records

Read operations.ts. Counts, throughput buckets and latency percentiles are database queries. The SSE stream sends those snapshots rather than generating decorative data. The dashboard also queries jobs, queues, workers and workflow records to render actual states.

Study pagination.ts and its use of cursor_created_at. JavaScript Date loses PostgreSQL's sub-millisecond precision; a cursor must preserve the original timestamp or it can skip rows. Display timestamps may be rounded without changing the traversal token.

## 10. Measure before optimizing

Run benchmarks/run.mjs against a dedicated queue as described in benchmarks/README.md. Compare no-op scheduling overhead, timed sleeps and numeric sum work. Vary one worker count at a time and repeat samples. Completion latency includes waiting and retries; client-observed completion throughput also includes HTTP/polling overhead. No single sample proves sustained capacity.

Potential next investigations are query plans, incremental metrics, starvation prevention, shared API rate limiting and PostgreSQL failover. Keep the durable lease contract unchanged while experimenting. Document an observed bottleneck before adding infrastructure.

## Validation policy

The owner's preference is to keep automated tests under ignored `.local/`, outside published Git history. This repository therefore ships build/type-check commands, benchmark tooling and a verification report, but not the local test bodies. That reduces independent reproducibility of the reported fault checks. The reported tests were actually executed; do not infer that GitHub CI runs them.
