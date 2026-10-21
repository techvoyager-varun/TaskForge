# Resume wording backed by this repository

- Built a distributed job queue and DAG workflow engine with C++20 schedulers, bounded worker pools, PostgreSQL lease fencing, retries and crash recovery, with a TypeScript API and React monitoring dashboard.
- Implemented shared Redis admission limits and version-validated job caching, plus a PostgreSQL transactional outbox, Kafka lifecycle stream and idempotent analytics consumer with namespaced replay.
- Published Docker-based C++ concurrency, API, PostgreSQL, tenant-isolation and fault-injection tests and GitHub Actions verification; measured worker/scheduler scaling, cache latency, event throughput and recovery using executable benchmarks.

These bullets describe implemented behavior verified by the public suite. If including numbers, qualify the workload: one short 80-job sample with two four-lane workers and two schedulers observed 34.82 completions/s for synthetic 10 ms sleep jobs on an i7-13700H developer machine. This is not sustained production throughput. A 100-read warm-cache sample averaged 12.21 ms versus 19.07 ms for 100 forced misses on a 49,921-byte response. Samples ran sequentially; do not generalize them into a universal percentage improvement.

Do not claim exactly-once job execution, production traffic, PostgreSQL/Kafka high availability, arbitrary-code sandboxing, per-user RBAC, TLS/SASL deployment, or sustained linear scaling. Explain one failure window and one limitation for each bullet in an interview. Start with INTERVIEW_GUIDE.md and the raw benchmark artifact.
