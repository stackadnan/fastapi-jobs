from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime

from fastapi_jobs.decorators import task
from fastapi_jobs.manager import JobManager
from fastapi_jobs.models import TERMINAL_STATUSES
from fastapi_jobs.worker import Worker


async def _run_until_terminal(
    worker: Worker, manager: JobManager, job_id: str, *, timeout: float = 5.0
):
    run = asyncio.create_task(worker.run())
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            job = await manager.get(job_id)
            if job.status in TERMINAL_STATUSES:
                return job
            await asyncio.sleep(0.02)
        raise AssertionError(f"job {job_id} never reached a terminal state within {timeout}s")
    finally:
        worker.request_shutdown()
        await asyncio.wait_for(run, timeout=2.0)


async def test_worker_runs_a_simple_task_to_success(manager: JobManager):
    @task
    async def add(a: int, b: int) -> int:
        return a + b

    job = await add.enqueue(2, 3)
    worker = Worker(manager, poll_interval=0.02)

    finished = await _run_until_terminal(worker, manager, job.id)

    assert finished.status.value == "SUCCESS"
    assert finished.result == 5
    assert finished.attempts == 1


async def test_worker_runs_sync_tasks_in_a_thread(manager: JobManager):
    @task
    def blocking_add(a: int, b: int) -> int:
        return a + b

    job = await blocking_add.enqueue(4, 5)
    worker = Worker(manager, poll_interval=0.02)

    finished = await _run_until_terminal(worker, manager, job.id)

    assert finished.status.value == "SUCCESS"
    assert finished.result == 9


async def test_worker_retries_and_eventually_succeeds(manager: JobManager):
    attempts = []

    @task(retries=3, backoff="fixed", base_delay=0.01, jitter=False)
    async def flaky() -> str:
        attempts.append(1)
        if len(attempts) < 3:
            raise ConnectionError("transient")
        return "done"

    job = await flaky.enqueue()
    worker = Worker(manager, poll_interval=0.02)

    finished = await _run_until_terminal(worker, manager, job.id)

    assert finished.status.value == "SUCCESS"
    assert finished.result == "done"
    assert finished.attempts == 3


async def test_worker_fails_permanently_after_exhausting_retries(manager: JobManager):
    @task(retries=2, backoff="fixed", base_delay=0.01, jitter=False)
    async def always_fails() -> None:
        raise ValueError("nope")

    job = await always_fails.enqueue()
    worker = Worker(manager, poll_interval=0.02)

    finished = await _run_until_terminal(worker, manager, job.id)

    assert finished.status.value == "FAILED"
    assert finished.attempts == 3
    assert finished.error is not None and "nope" in finished.error


async def test_worker_enforces_task_timeout(manager: JobManager):
    @task(timeout=0.1)
    async def too_slow() -> None:
        await asyncio.sleep(5)

    job = await too_slow.enqueue()
    worker = Worker(manager, poll_interval=0.02)

    started = time.monotonic()
    finished = await _run_until_terminal(worker, manager, job.id, timeout=3.0)
    elapsed = time.monotonic() - started

    assert finished.status.value == "FAILED"
    assert finished.error is not None and "timeout" in finished.error.lower()
    assert elapsed < 2.0


async def test_worker_fails_jobs_for_tasks_it_has_no_code_for(manager: JobManager):
    # A worker process that hasn't imported the module defining a task can still
    # claim a job for it (the queue doesn't know about Python imports) -- it should
    # fail the job loudly instead of crashing.
    job = await manager.backend.create_job(
        "not_a_real_task",
        '{"args": [], "kwargs": {}}',
        max_attempts=1,
        timeout_seconds=5,
        available_at=datetime.now(UTC),
    )
    worker = Worker(manager, poll_interval=0.02)

    finished = await _run_until_terminal(worker, manager, job.id)

    assert finished.status.value == "FAILED"
    assert finished.error is not None and "not_a_real_task" in finished.error
