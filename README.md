# TaskForge

A PostgreSQL-backed distributed job queue and workflow engine with a C++20 scheduler and workers, an Express API, and a React operations dashboard.

## Development status

Implementation is in progress. See [the design](docs/architecture/execution-model.md) and [delivery plan](docs/implementation-plan.md) for the execution model and acceptance gates. Features are documented as available only after verification.

## Engineering principles

- PostgreSQL is the durable source of truth.
- Scheduling decisions belong to the C++ scheduler.
- Lease ownership is fenced by an increasing attempt number.
- Execution is at least once; handlers must protect external side effects with idempotency.
- Each commit represents one coherent, checked increment.
- Automated tests stay in the Git-ignored `.local/` directory at the repository owner's request. This intentionally limits clean-checkout test reproducibility; builds and type checks remain reproducible.
