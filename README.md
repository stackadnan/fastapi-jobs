# fastapi-jobs

A lightweight, FastAPI-native background job system for the space between FastAPI's built-in `BackgroundTasks` and Celery.

`BackgroundTasks` is great for simple fire-and-forget work, while Celery can introduce more infrastructure than a small FastAPI project needs. `fastapi-jobs` aims to provide the middle ground.

## Features

* Async background jobs
* FastAPI integration
* SQLite backend
* Task registration
* Job enqueueing
* Separate worker process
* Configurable retries
* Exponential backoff
* Task timeouts
* Crash recovery through lease expiry
* At-least-once delivery semantics
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

## Cancellation

Currently, cancellation is supported only for jobs that have not started running.

Jobs that are already being executed cannot currently be cancelled.

## Current Status

`fastapi-jobs` is under active development.

Currently implemented:

* Task registration
* Job enqueueing
* SQLite backend
* Polling worker
* Job leasing
* Lease expiry
* Crash recovery
* Task timeouts
* Retry handling
* Exponential backoff
* Basic cancellation for pending jobs

Not implemented yet:

* Redis backend
* Optional FastAPI job router
* Web dashboard
* Advanced job scheduling
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
* [ ] Job status API
* [ ] Job cancellation
* [ ] Job scheduling
* [ ] Recurring jobs
* [ ] Web dashboard
* [ ] Multiple worker support improvements
* [ ] Dead-letter jobs
* [ ] Job result storage
* [ ] Metrics and observability
* [ ] Production deployment documentation

## License

MIT
