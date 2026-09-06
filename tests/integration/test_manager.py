from __future__ import annotations

import pytest

from fastapi_jobs.decorators import task
from fastapi_jobs.exceptions import JobAlreadyCompletedError, JobNotFoundError
from fastapi_jobs.manager import JobManager
from fastapi_jobs.models import JobStatus


async def test_enqueue_creates_a_pending_job_with_the_tasks_policy(manager: JobManager):
    @task(retries=2, timeout=45)
    async def charge_card(amount: int) -> None:
        pass

    job = await charge_card.enqueue(500)

    assert job.status is JobStatus.PENDING
    assert job.args == (500,)
    assert job.max_attempts == 3
    assert job.timeout_seconds == 45


async def test_enqueue_falls_back_to_config_default_timeout(manager: JobManager):
    @task
    async def no_explicit_timeout() -> None:
        pass

    job = await no_explicit_timeout.enqueue()

    assert job.timeout_seconds == manager.config.default_timeout


async def test_get_missing_job_raises(manager: JobManager):
    with pytest.raises(JobNotFoundError):
        await manager.get("missing-id")


async def test_cancel_pending_job_succeeds(manager: JobManager):
    @task
    async def do_nothing() -> None:
        pass

    job = await do_nothing.enqueue()
    cancelled = await manager.cancel(job.id)
    assert cancelled.status is JobStatus.CANCELLED


async def test_cancel_running_job_requests_cooperative_cancellation(manager: JobManager):
    @task
    async def do_nothing() -> None:
        pass

    job = await do_nothing.enqueue()
    await manager.backend.claim_job("worker-1", lease_buffer_seconds=30)

    result = await manager.cancel(job.id)

    # Nothing can forcibly stop a running task from the outside -- cancelling it
    # flags the job and returns immediately. The worker holding its lease is what
    # actually cancels the task and settles the job into CANCELLED; see
    # tests/integration/test_cancellation.py for that end-to-end path.
    assert result.status is JobStatus.RUNNING
    assert result.cancel_requested_at is not None


async def test_cancel_terminal_job_raises_already_completed(manager: JobManager):
    @task
    async def do_nothing() -> None:
        pass

    job = await do_nothing.enqueue()
    await manager.backend.claim_job("worker-1", lease_buffer_seconds=30)
    await manager.backend.complete_job(job.id, "worker-1", result=None)

    with pytest.raises(JobAlreadyCompletedError):
        await manager.cancel(job.id)


async def test_list_filters_by_task_name(manager: JobManager):
    @task(name="task-a")
    async def a() -> None:
        pass

    @task(name="task-b")
    async def b() -> None:
        pass

    await a.enqueue()
    await b.enqueue()
    await b.enqueue()

    only_b = await manager.list(task_name="task-b")
    assert len(only_b) == 2
    assert all(job.task_name == "task-b" for job in only_b)
