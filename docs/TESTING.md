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

The suite also exercises Redis shared quotas under concurrent replicas, stale/foreign cache entries, transport loss/reconnect and noeviction memory exhaustion. Real Kafka tests cover outbox rollback/ack loss, failed publication and producer restart, duplicate/out-of-order projection effects, poison schemas, consumer stop/restart and both Kafka/retained-SQL replay. The stream test verifies active SSE responses end during API shutdown.

On this Docker/WSL host, default PIE ASan executables sometimes failed during sanitizer initialization; an empty `int main(){return 0;}` program reproduced the failure. A non-PIE ASan debug build passed the C++ suite and all 11 core PostgreSQL/process checks. Reproduce the workaround without changing production executables:

```sh
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DTASKFORGE_SANITIZER=address -DCMAKE_CXX_FLAGS=-fno-pie -DCMAKE_EXE_LINKER_FLAGS=-no-pie
cmake --build build-asan -j2
ctest --test-dir build-asan --output-on-failure
TEST_CPP_DIR=build-asan npm run test:integration
```

Run the latter inside the test image/network with TEST_DATABASE_URL/TEST_REDIS_URL/KAFKA_BROKERS configured, as in compose.test.yml. ThreadSanitizer on the local WSL host reports an unexpected memory mapping even for an empty program; that is an unsupported runtime result, not a race-free claim. CI still builds and runs the sanitizer matrix and only annotates that exact unsupported condition. Any other failure fails the job.
