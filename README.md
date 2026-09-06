# fastapi-jobs

[![PyPI version](https://img.shields.io/pypi/v/fastapi-jobs.svg)](https://pypi.org/project/fastapi-jobs/)
[![Python versions](https://img.shields.io/pypi/pyversions/fastapi-jobs.svg)](https://pypi.org/project/fastapi-jobs/)
[![License: MIT](https://img.shields.io/pypi/l/fastapi-jobs.svg)](https://github.com/stackadnan/fastapi-jobs/blob/main/LICENSE)

A lightweight, FastAPI-native background job system for the space between FastAPI's built-in `BackgroundTasks` and Celery.

`BackgroundTasks` is great for simple fire-and-forget work, while Celery can introduce more infrastructure than a small FastAPI project needs. `fastapi-jobs` aims to provide the middle ground.

## Features

* Async background jobs
* FastAPI integration
* SQLite backend
* Task registration
* Job enqueueing
* Separate worker process
* Configurable worker concurrency
* Configurable retries
* Exponential backoff
* Task timeouts
* Crash recovery through lease expiry
* At-least-once delivery semantics
* Cancellation of running jobs
* Completed-job retention and cleanup
* Lightweight architecture

## Quick Start

Install the package:

```bash
pip install fastapi-jobs
```

Create a FastAPI application:

```python
from fastapi import FastAPI

from fastapi_jobs import Jobs, task

app = FastAPI()

jobs = Jobs(
    app,
    database="sqlite:///jobs.db",
)


@task(retries=3, timeout=60)
async def send_email(user_id: int) -> None:
    print(f"Sending email to user {user_id}")


@app.post("/users/{user_id}/welcome")
async def welcome(user_id: int):
    job = await send_email.enqueue(user_id)

    return {"job_id": job.id}
```

## Running the Worker

The API process only enqueues jobs. A separate worker process executes them.

Create a worker:

```python
import asyncio

from fastapi_jobs import Jobs, Worker


jobs = Jobs(database="sqlite:///jobs.db")


if __name__ == "__main__":
    asyncio.run(Worker(jobs.manager).run())
```

Run the worker:

```bash
python worker.py
```

You can then run your FastAPI application normally:

```bash
uvicorn main:app --reload
```

### Concurrency

By default a worker runs one job at a time. Pass `max_concurrency` to let it hold several jobs in flight:

```python
Worker(jobs.manager, max_concurrency=4)
```

The worker keeps claiming jobs up to that limit, so a slow job no longer blocks everything else queued behind it. `max_concurrency` must be at least 1; the default of 1 matches the previous single-job behavior, so existing code that doesn't pass it is unaffected.

## How It Works

The basic flow is:

```text
FastAPI Request
      |
      v
 enqueue()
      |
      v
   Database
      |
      v
    Worker
      |
      v
 Execute Task
      |
      v
 Record Result
```

The worker continuously polls for available jobs and claims them using a lease.

If a worker crashes while processing a job, the lease eventually expires and another worker can pick up the job.

## Delivery Semantics

Jobs use **at-least-once delivery**.

A job can potentially execute more than once.

For example, if a worker successfully completes a task but crashes before recording the result, the job lease will eventually expire. Another worker can then execute the same job again.

Because of this, tasks should be designed to be safe when executed more than once.

For example, instead of blindly creating a payment every time a job runs, use an idempotency key such as the job ID.

```python
@task(retries=3)
async def process_payment(job_id: str, amount: int):
    # Use job_id as an idempotency key
    ...
```

Exactly-once execution is intentionally not guaranteed.

## Retries

Tasks can define the number of retries:

```python
@task(retries=3)
async def send_email(user_id: int): ...
```

When a task fails, the worker retries it using exponential backoff.

This helps prevent repeatedly hammering an external service when it is temporarily unavailable.

## Timeouts

Tasks can also define a timeout:

```python
@task(timeout=60)
async def generate_report(report_id: int): ...
```

If the task exceeds the configured timeout, the worker terminates the attempt and handles it according to the job's retry configuration.

## Retention

By default, completed jobs (`SUCCESS`, `FAILED`, `CANCELLED`) are kept forever. For a long-running process this means the database grows without bound.

Retention is opt-in. Pass `retention` to a worker to have it periodically delete terminal jobs older than that:

```python
from datetime import timedelta

worker = Worker(
    jobs.manager,
    retention=timedelta(days=7),
    retention_check_interval=3600,  # how often to check, in seconds (default: hourly)
)
```

PENDING, RETRYING, and RUNNING jobs are never purged, regardless of age. Upgrading to a version of `fastapi-jobs` that supports retention does not start deleting anything on its own -- it only runs if you explicitly configure it.

If you'd rather control cleanup yourself (a cron job, a management command, a different schedule per environment), call the backend directly instead of configuring it on a `Worker`:

```python
deleted = await jobs.backend.purge_jobs(older_than=timedelta(days=7))
```

## Cancellation

A PENDING or RETRYING job is cancelled immediately:

```python
job = await manager.cancel(job_id)
# job.status is JobStatus.CANCELLED
```

A RUNNING job is cancelled cooperatively. Calling `cancel()` on it flags the job and returns right away with the job still shown as RUNNING:

```python
job = await manager.cancel(job_id)
# job.status is still JobStatus.RUNNING
# job.cancel_requested_at is now set
```

The worker holding that job's lease notices the flag on its next poll, cancels its local `asyncio.Task`, and the job settles into `CANCELLED` once that unwinds -- typically within one `poll_interval`. Poll `manager.get(job_id)` (or `GET /jobs/{id}` if the REST API is mounted) to see it land. A cancelled job is never retried, no matter how many retries it had left.

This only interrupts an `await` inside the task -- an `async def` task cancels as soon as it next hits one. A sync task, which runs in a worker thread via `asyncio.to_thread`, cannot be forcibly stopped: cancelling it marks the job `CANCELLED` right away, but the thread itself keeps running the function to completion in the background, since Python has no safe way to kill a running thread. Write sync tasks that either finish quickly or check for cancellation themselves if this matters for your use case.

## Current Status

`fastapi-jobs` is published on PyPI and under active development.

Currently implemented:

* Task registration
* Job enqueueing
* Delayed job scheduling (`enqueue(..., delay=...)`)
* SQLite backend
* Polling worker with configurable concurrency (`max_concurrency`)
* Job leasing
* Lease expiry
* Crash recovery
* Task timeouts
* Retry handling
* Exponential backoff
* Cancellation of pending jobs (immediate) and running jobs (cooperative)
* Completed-job retention and cleanup, opt-in via `Worker(retention=...)` or `backend.purge_jobs()`
* Optional REST API for job inspection and cancellation (`Jobs(app, api=True)`)

Not implemented yet:

* Redis backend
* Web dashboard
* Cron-style recurring jobs

## Requirements

* Python 3.11+
* FastAPI

## Development

Clone the repository:

```bash
git clone https://github.com/stackadnan/fastapi-jobs.git
cd fastapi-jobs
```

Install development dependencies:

```bash
pip install -e ".[dev]"
```

Run tests:

```bash
pytest
```

Run Ruff:

```bash
ruff check .
```

Run mypy:

```bash
mypy fastapi_jobs
```

## Project Goals

The goal of `fastapi-jobs` is not to replace Celery.

It is intended for FastAPI applications that need more reliability than `BackgroundTasks` provides, but do not want to introduce a large distributed task processing stack.

The project aims to remain:

* Simple
* Fast
* Python-native
* Async-friendly
* Easy to deploy
* Easy to understand
* Easy to extend

## Roadmap

* [ ] Redis backend
* [ ] PostgreSQL backend
* [x] Job status API (opt-in REST endpoints)
* [x] Job cancellation (pending jobs immediately, running jobs cooperatively)
* [x] Delayed job scheduling
* [x] Configurable worker concurrency
* [x] Job retention and cleanup
* [ ] Recurring jobs
* [ ] Web dashboard
* [ ] Dead-letter jobs
* [x] Job result storage
* [ ] Metrics and observability
* [ ] Production deployment documentation

## License

MIT
