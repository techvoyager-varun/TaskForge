# Verification record — 21 October 2026

All implementation work occurred in E:\TaskForge. The repository began empty. Changes were committed and pushed at coherent milestones using actual commit timestamps. Automated test bodies, fixture credentials, binaries and transient logs remain in ignored `.local/`, per the owner's instruction; none is part of Git history.

## Build and deployment checks

| Check | Observed result |
| --- | --- |
| npm run typecheck | API, scripts, contracts and dashboard passed strict TypeScript checking |
| npm run build | API/admin bundles and Vite production assets built successfully |
| CMake debug build with -Wall -Wextra -Wpedantic -Werror | Scheduler and worker compiled with GCC 12.2 |
| Docker release builds | API/dashboard and both C++ executables built from source |
| Fresh Compose database | Migrations and independent service logins provisioned successfully |
| Upgrade to migration 003 | Applied successfully; migration ledger lists 001, 002 and 003 |
| Repeated bootstrap | No duplicate schema application; existing checksums accepted |
| Final Compose HTTP check | Authenticated sum job returned 60; metrics and SSE returned actual durable data |
| Final health | API and PostgreSQL healthy on loopback port 3000; two workers and two schedulers running |
| npm audit --omit=dev --audit-level=high | Zero reported production dependency vulnerabilities |
| GitHub Actions | Published code commit 3c56696 passed TypeScript/build/audit and C++ normal/address/thread build jobs |

## Local behavioral checks

The final post-review suite ran the real PostgreSQL schema and C++ processes, rather than an in-memory queue substitute.

- Database invariants: durable assignment/completion, queue capacity, invalid transitions, increasing fences, expired completion rejection before reassignment and stale completion rejection after reassignment.
- Recovery: killed a running worker; its job completed on a later fenced attempt (attempt 3 in the observed run).
- Competition: two schedulers and two workers completed 30 sleep jobs with one attempt per job; sampled queue occupancy never exceeded its configured limit of two and reached two.
- Reliability: permanent failures exhausted exactly two attempts; a transient failure succeeded on attempt two; a 200 ms attempt deadline produced TIMED_OUT and dead-lettering; a running sleep cancelled cooperatively.
- Process loss: one scheduler was killed while another completed a new job; five persisted pending jobs survived PostgreSQL restart and completed afterward.
- HTTP boundary: missing/invalid token rejected, another project's job read/cancel returned 404, same idempotency key/request returned the same ID, different request with that key returned 409, arbitrary shell handler/invalid payload/invalid pagination limit rejected.
- Workflows: a draft had no attempts; a diamond completed with every child assigned after each parent's completion; retries created linked separate executions; a failed root cancelled three descendants; workflow cancellation completed.
- Cursor precision: twenty jobs within one millisecond were traversed in pages of five without skipped or duplicated IDs.
- Streams: eleven simultaneous opens produced ten accepted connections and one 429; token rotation rejected new HTTP immediately and closed its existing stream within a 35-second observation window.
- Service privileges: API role could not dispatch, worker role could not directly read jobs, scheduler role could not read projects.
- Retention: dry run preserved data; apply removed an eligible old standalone job and its dependent records while preserving active future work; final Compose dry run found zero eligible records.
- GoogleTest: three local C++ tests passed for rejecting an empty pool, executing callbacks, bounding the pending queue and interrupting/joining lanes on shutdown.

One repeated fault-run invocation initially collided with a pre-existing disposable worker container. The stopped fixture container was removed and the complete process/regression suite was rerun successfully. This was a harness setup failure, not a reported product pass.

## Review-driven regressions

An independent source review found three issues; each received a local reproducer before correction, and the focused corrections were reviewed again.

1. Assignment acceptance: a local copy of the installed function's exact body added only an advisory timing gate before SELECT LockRows. With READ COMMITTED snapshots, two lanes returned the same current assignment before the correction. Migration 003's status-qualified update caused the second acceptance to return no assignment afterward. This checks acceptance in addition to completion fencing.
2. Workflow cancellation: while the maintenance advisory lock was held, the old endpoint proceeded. The corrected endpoint waited without owning workflow/job row locks, then completed after release. Sharing maintenance's lock before acquiring rows removes its lock-order inversion.
3. Manual retry quota: a rollback-only fixture with 10,000 active jobs returned READY before the fix and QUOTA after. The API maps that result to HTTP 429.

The initial idea of a repeatable-read acceptance reproducer was discarded because PostgreSQL could raise serialization failure even before the correction. It was not counted as proof of the defect; the READ COMMITTED timing-gate check provided that evidence.

## Memory and concurrency instrumentation

AddressSanitizer builds compiled both executables. An instrumented worker completed a real persisted job and joined its lanes during graceful shutdown; the exercised run emitted no AddressSanitizer or LeakSanitizer report. This does not prove every execution path is memory-safe.

ThreadSanitizer builds compiled, including GitHub CI. The local runtime exited before workload execution with `FATAL: ThreadSanitizer: unexpected memory mapping`. Runtime race detection therefore remains unverified in this Docker environment. There is no claim that a ThreadSanitizer workload passed.

## Browser checks

The production dashboard was opened through the in-app browser. It displayed actual project counts, queue capacity and worker association. Submitting an echo job from the form produced a completed result and durable attempt/log display. Creating and running the supplied four-node diamond produced completed graph nodes and execution history. The final built application serves its project-token connection screen at port 3000. A temporary local QA token was revoked after the browser checks.

## Measurements and limits

Four short 80-job samples completed, with raw output in benchmarks/results and methodology in benchmarks/README.md. They cover one/two worker no-ops, timed sleep and numeric computation, with zero extra attempts and zero throttled requests in those runs. Measurements were collected during implementation before the final coordination hardening; they are baseline observations, not a benchmark certification of every subsequent commit.

The test bodies are intentionally unpublished, so a clean clone can reproduce build/deployment/benchmark commands but cannot independently rerun the private regression suite. Arbitrary TCP blackholes, multi-host clock/failure behavior, database failover, high-volume sustained performance, and exhaustive interleavings have not been established by these checks.
