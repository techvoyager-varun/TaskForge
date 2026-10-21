# Verification entry point

The latest audit explicitly supersedes the earlier local-only test policy. Automated tests are public in `tests/`; GitHub Actions executes contract, C++ concurrency and isolated PostgreSQL/Redis/Kafka integration coverage. No credentials or `.local/` artifacts are required on a clean checkout.

See [TESTING.md](TESTING.md) for commands and [FINAL_ENGINEERING_REPORT.md](FINAL_ENGINEERING_REPORT.md) for executed results, sanitizer caveats and production limitations. Raw measured samples live in `benchmarks/results/2026-10-21-audit.json`; `benchmarks/audit.ts` is their executable workload. The older four benchmark files describe an earlier pre-Redis/Kafka baseline and are not current capacity claims.
