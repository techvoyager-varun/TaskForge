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

Host: Intel Core i7-13700H, 20 logical processors, 15.7 GiB RAM, Windows 10.0.26300, Docker Desktop Linux engine 29.8.0. Node client v22.17.0. PostgreSQL 17.11; C++ release build, GCC 12.2, libpq 15.19. Two scheduler processes; four execution lanes per worker; queue limit 8; eight HTTP producers; 80 jobs per sample. A separate development stack was also present on the host. These were short single samples without dedicated-machine isolation or a warm-up protocol.

| Workload | Workers | Submission jobs/s | HTTP-observed completions/s | Completion p50 / p95 / p99 (ms) |
| --- | ---: | ---: | ---: | --- |
| Synthetic no-op | 1 | 74.03 | 27.85 | 1412 / 1933 / 2023 |
| Synthetic no-op | 2 | 95.52 | 46.36 | 710 / 847 / 908 |
| Synthetic 25 ms sleep | 2 | 82.45 | 23.35 | 815 / 1083 / 1135 |
| Sum 10,000 numbers | 2 | 30.76 | 28.32 | 338 / 431 / 464 |

All four samples completed 80/80 jobs with zero extra attempts and zero rate-limited requests. Exact observations and UTC start times are in results/. Latencies use durable created_at and finished_at timestamps rounded to milliseconds by the HTTP JSON representation. Completion throughput is count divided by client-observed wall time; it includes polling and HTTP delays and is not a server-only execution rate. Completion latency and this wall-time rate measure different intervals.

These observations do not establish sustained capacity, a general scaling factor, a database saturation point, scheduler CPU cost under load, or production suitability. No-op and sleep work are synthetic overhead measurements. The sum sample does actual numeric work but is still a small microbenchmark. Repeated longer runs, error bars, CPU/memory sampling during load, and database query analysis remain future performance work.
