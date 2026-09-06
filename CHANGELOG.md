# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.2.0]

A production-readiness release: workers can run jobs concurrently instead of one at a
time, running jobs can be cancelled, old jobs can be cleaned up automatically, and CI
now runs on every push and pull request instead of only at release time.

### Added

- `Worker(..., max_concurrency=N)`: a worker holds up to `N` jobs in flight at once
  instead of executing them one at a time. Defaults to `1`, matching the previous
  behavior, so existing code is unaffected unless it opts in.
- Cancellation of running jobs. `manager.cancel()` on a RUNNING job now flags it for
  cooperative cancellation instead of raising -- the owning worker cancels its local
  task on its next poll and the job settles into `CANCELLED`. `Job.cancel_requested_at`
  and the REST API's `JobOut.cancel_requested_at` expose whether a cancellation is
  pending. Cancelled jobs are never retried.
- `JobBackend.purge_jobs(older_than=...)` deletes terminal (`SUCCESS`/`FAILED`/
  `CANCELLED`) jobs whose `finished_at` is older than the given `timedelta`. PENDING,
  RETRYING, and RUNNING jobs are never touched.
- `Worker(..., retention=timedelta(...), retention_check_interval=...)`: opt-in
  periodic purging on the worker's own poll loop. Retention is off by default, so
  upgrading doesn't start deleting historical jobs on its own.
- CI now runs the test suite, Ruff, and mypy on every push to `main` and on every pull
  request, across Python 3.11-3.13, instead of only when a release is published.

### Changed

- `manager.cancel()` no longer raises `InvalidJobStateError` for a job that is
  currently RUNNING, or for one that started running in the moment between reading
  its status and cancelling it -- both cases now request cooperative cancellation
  instead. Cancelling a job already in a terminal state still raises
  `JobAlreadyCompletedError`.
- A worker that fails to record a job's outcome because another worker had already
  reclaimed its lease now logs a warning and moves on, instead of leaving the
  exception unhandled in a background task.

### Removed

- The unused `dashboard` optional dependency (`pip install fastapi-jobs[dashboard]`).
  No dashboard code existed to back it; a real one is tracked under future work
  instead of shipping a phantom extra.

### Known limitations

- Cancellation only interrupts an in-progress `await`. A sync task already running in
  its worker thread cannot be forcibly stopped -- the job is marked `CANCELLED`
  immediately, but the thread keeps running the function to completion in the
  background. See the README's Cancellation section.
- Redis backend, cron-style recurring jobs, and a web dashboard remain future work.

[0.2.0]: https://github.com/stackadnan/fastapi-jobs/releases/tag/v0.2.0

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
