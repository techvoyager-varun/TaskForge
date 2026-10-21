# TaskForge implementation plan

Goal: establish the smallest durable execution path first, then reliability, workflows and real observability.
Architecture: Express submits to PostgreSQL; a C++20 scheduler grants fenced leases; bounded C++ workers execute allowlisted handlers. SQL transactions enforce the shared state machine.
Stack: C++20, CMake, libpq, PostgreSQL 17, Node.js 22, strict TypeScript, Express, Zod, React, Vite.
Spec: architecture/execution-model.md; requirements supplied in E:\TaskForge.md.

## Constraints and rulings

- All work and generated tools stay inside E:\TaskForge.
- Commit coherent increments with honest timestamps; never fabricate development history.
- Automated tests live under ignored .local/ and are never staged, per the owner's latest instruction.
- Keep the existing main checkout, as explicitly requested; do not create another worktree.
- Execute directly in this chat under the user's instruction to start.
- No fabricated benchmark data or claims that unrun checks passed.

## Review focus

Expired completion before reassignment; two schedulers at capacity boundaries; malformed/oversized payloads; idempotency reuse across queues; database loss during acknowledgement. Each is covered by a local behavioral check in its owning increment.

## Delivery sequence

1. Architecture and repository conventions. Review documents and commit.
2. Persistence and contracts: write failing local database checks; implement schema, transitions, idempotency and lease procedures; run checks; commit.
3. C++ scheduler and worker: write failing local process checks; implement RAII libpq wrapper, thread pool, handler registry, assignment loop and shutdown; compile and run a real persisted job; commit.
4. Express API: write failing local HTTP checks; implement token authentication, validation, project-scoped routes and migration/bootstrap commands; type check and run checks; commit.
5. Reliability and workflows: extend local checks with worker kill/restart, two schedulers, lease fencing, cancellation, retries and DAG validation; implement remaining behaviors; run checks; commit.
6. Dashboard and packaging: add real job/worker/queue/workflow views, Docker Compose, CI compilation/type checking and operational documentation; build and exercise the application; commit.
7. Run fault checks and a measured load experiment. Review diff and credential hygiene, document actual results and remaining work; final focused commit.

## Local verification record

Record commands, observed outcomes, limitations and deviations in docs/verification.md after running them. Test bodies and transient logs remain ignored in .local/.
