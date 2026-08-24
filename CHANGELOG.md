# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [0.1.0]

Initial release.

### Added

- `@task` decorator and `Task.enqueue()` for defining and queuing background jobs.
- SQLite backend with lease-based job claiming: a worker's expired lease lets another
  worker reclaim its job, and compare-and-swap writes stop a worker that's lost its
  lease from clobbering whoever reclaimed it.
- Polling `Worker` with configurable retries, exponential/fixed backoff with jitter,
  per-task timeouts, and graceful shutdown on SIGTERM/SIGINT.
- `Jobs(app, database=...)` FastAPI integration.
- Optional REST API (`Jobs(app, api=True)`) exposing `GET /jobs/{id}`, `GET /jobs`,
  and `POST /jobs/{id}/cancel`, opt-in and configurable via `ApiConfig`.
- Job cancellation for jobs that haven't started running yet (`PENDING`/`RETRYING`).
- Delayed enqueueing via `enqueue(..., delay=...)`.

### Known limitations

- Execution is at-least-once, not exactly-once; see the README's Delivery Semantics
  section.
- Cancellation of an already-running job isn't supported yet.
- Redis backend and the web dashboard aren't implemented yet.

[0.1.0]: https://github.com/stackadnan/fastapi-jobs/releases/tag/v0.1.0
