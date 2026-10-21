# Reproducible workload measurements

Use a dedicated empty queue. Create it through POST /api/v1/queues with concurrency_limit 8. Set TASKFORGE_TOKEN and TASKFORGE_QUEUE in your shell environment; never put them in tracked files.

```powershell
$env:TASKFORGE_URL = 'http://127.0.0.1:3000'
$env:TASKFORGE_TOKEN = '<your project token>'
$env:TASKFORGE_QUEUE = '<dedicated queue UUID>'
$env:BENCH_COUNT = '80'
$env:BENCH_CONCURRENCY = '8'
node benchmarks/run.mjs
```

Set BENCH_HANDLER to sum to compute the sum of 10,000 numbers in every job, or to sleep with BENCH_DURATION_MS=25 for timed synthetic work. The sum workload checks a returned result against 50,005,000. BENCH_OUTPUT optionally writes JSON to a local path. Count is deliberately capped at 100 so completion polling needs one API page. Do not run concurrent producers into the benchmark queue. The API's 300 requests/minute limit remains enabled; the report counts throttled requests and includes their waiting time.

## Observed on 21 October 2026

Host: Intel Core i7-13700H, 20 logical processors, 15.7 GiB RAM, Windows 10.0.26300, Docker Desktop Linux engine 29.8.0. Docker reported 20 CPUs and approximately 7.6 GiB allocated memory. Node client v22.17.0. PostgreSQL 17.11; C++ release build, GCC 12.2, libpq 15.19. Two scheduler processes; four execution lanes per worker; queue limit 8; eight HTTP producers; 80 jobs per sample. A separate development stack was also present on the host. These were short single samples without dedicated-machine isolation or a warm-up protocol.

| Workload              | Workers | Submission jobs/s | HTTP-observed completions/s | Completion p50 / p95 / p99 (ms) |
| --------------------- | ------: | ----------------: | --------------------------: | ------------------------------- |
| Synthetic no-op       |       1 |             74.03 |                       27.85 | 1412 / 1933 / 2023              |
| Synthetic no-op       |       2 |             95.52 |                       46.36 | 710 / 847 / 908                 |
| Synthetic 25 ms sleep |       2 |             82.45 |                       23.35 | 815 / 1083 / 1135               |
| Sum 10,000 numbers    |       2 |             30.76 |                       28.32 | 338 / 431 / 464                 |

All four samples completed 80/80 jobs with zero extra attempts and zero rate-limited requests. Exact observations and UTC start times are in results/. Latencies use durable created_at and finished_at timestamps rounded to milliseconds by the HTTP JSON representation. Completion throughput is count divided by client-observed wall time; it includes polling and HTTP delays and is not a server-only execution rate. Completion latency and this wall-time rate measure different intervals.

Measurements were recorded during implementation before the final coordination hardening; rerun the current checkout before comparing changes. These observations do not establish sustained capacity, a general scaling factor, a database saturation point, scheduler CPU cost under load, or production suitability. No-op and sleep work are synthetic overhead measurements. The sum sample does actual numeric work but is still a small microbenchmark. Repeated longer runs, error bars, CPU/memory sampling during load, and database query analysis remain future performance work.

## Current Redis/Kafka audit workload

The older four JSON artifacts predate Redis/Kafka and are historical samples. The current executable is `benchmarks/audit.ts`, with raw data in `results/2026-10-21-audit.json`.

```sh
docker compose -f compose.test.yml up -d db redis kafka
docker compose -f compose.test.yml build tests
docker compose -f compose.test.yml run --rm tests node --import tsx benchmarks/audit.ts
```

It requires an explicitly test-named database. The image runs real C++ processes and the production API implementation in the isolated network. To retain new output on the host, mount the repository's benchmarks directory at `/source/benchmarks` (Windows: `-v E:/TaskForge/benchmarks:/source/benchmarks`). Existing fixture events are temporarily excluded from the measured publishing batch. Use a disposable test stack and rerun the public suite before reusing it. The benchmark's high admission threshold is local to its API instance; production remains300/IP/minute.

Measured environment: Docker Desktop Linux/WSL2 on Windows, i7-13700H, 20 visible logical CPUs, 7.62 GiB visible RAM, Node22.23.4, GCC12 C++20 Debug, PostgreSQL17, Redis7.4, Kafka3.9.1. Queue limit8, four lanes/worker, eight concurrent HTTP submissions, 80 synthetic sleep10ms jobs per configuration.

| Workers | Schedulers | Submit jobs/s | Observed completion jobs/s | Completion p95 ms | Queue wait p95 ms |
| ------- | ---------- | ------------- | -------------------------- | ----------------- | ----------------- |
| 1       | 1          | 44.45         | 20.15                      | 2165.11           | 2117.18           |
| 2       | 1          | 72.13         | 27.77                      | 1750.65           | 1717.38           |
| 1       | 2          | 69.19         | 18.70                      | 1673.13           | 1592.24           |
| 2       | 2          | 75.82         | 34.82                      | 1306.33           | 1283.16           |

All four samples completed without extra attempts or observed database deadlocks. Sampled peak lock waiters were0,1,0,0; short sampling can miss contention. Two schedulers were slower in the one-worker sample, so this does not establish linear scaling or a universal benefit from replicas. Polling/transactions dominate a10ms synthetic handler. See raw JSON for p50/p95/p99 submission, completion, waiting and execution latency.

A49,921-byte sum job-detail response measured100 warm hits then100 forced misses: mean12.21ms versus19.07ms; p95 20.06ms versus29.34ms. Both paths use the same Redis limiter/authentication; key deletion occurs outside miss timing. This supports a benefit for this payload/run but not statistical significance or sustained capacity. Version checks retain one PostgreSQL read/hit.

The publisher drained1,619 events in17 batches over0.784s (2,064.09 events/s), including SQL claim and post-ack update. The consumer reached zero observed lag in5.480s including group startup (295.45 observed events/s). This is one local broker, three partitions, replication1 and acks=all. It is not a broker capacity test.

One killed-worker sample recovered to durable completion in15.66s with three attempts for a2s sleep handler. The dead worker's still-recent heartbeat permitted another wasted assignment before retry; six-second leases do not bound total recovery time.

For the separate complete-stack outage demonstration, set TASKFORGE_TOKEN/TASKFORGE_QUEUE for a disposable local project and run `node benchmarks/stack-faults.mjs`. It really stops/restarts publisher, Kafka and Redis and runs a diamond DAG, then restores services. `results/2026-10-21-stack-faults.json` records the executed outcomes without credentials. This command intentionally disrupts the local demo stack; it is not a production load test.
