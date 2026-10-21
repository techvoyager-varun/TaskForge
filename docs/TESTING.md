# Reproducible verification

The current engineering audit supersedes the earlier local-only testing policy. Tests in `tests/` are intentionally public and contain no credentials or production endpoints.

On a clean checkout with Docker Compose:

```sh
docker compose -f compose.test.yml up --build --abort-on-container-exit --exit-code-from tests
docker compose -f compose.test.yml down -v
```

This creates the isolated `taskforge-test` project with no published database port. The test database uses trust authentication only within this disposable network. Never point the integration suite at a real database: it requires a test-named database and truncates test fixtures between cases.

Coverage includes C++ bounded pool concurrency and cooperative shutdown, API validation, idempotency, tenant isolation, persisted C++ handler results, DAG dependencies and cancellation, microsecond pagination, stream quotas, assignment snapshot races, SQL fencing/lease/retry invariants, two schedulers with two workers, crash recovery, and an established TCP connection that silently drops all PostgreSQL traffic. The latter must hit the C++ client deadline instead of hanging.

`npm test` runs pure contract tests without infrastructure. CMake/CTest needs GTest, libpq, nlohmann-json, and a C++20 compiler. Configure `-DTASKFORGE_SANITIZER=address` or `thread` for sanitizer instrumentation. CI executes these tests; an unsupported ThreadSanitizer address-space mapping is explicitly annotated as unsupported, never described as a successful runtime check.

Assertions are not replaced with demo values. Failure output includes real process logs (without connection strings). See the final engineering report for executed results and environment limits.
