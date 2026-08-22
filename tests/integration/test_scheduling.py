from __future__ import annotations

from datetime import UTC, datetime

from fastapi_jobs.decorators import task
from fastapi_jobs.manager import JobManager


async def test_delay_pushes_available_at_into_the_future(manager: JobManager):
    @task
    async def send_reminder() -> None:
        pass

    before = datetime.now(UTC)
    job = await send_reminder.enqueue(delay=60)

    assert job.available_at > before
    assert (job.available_at - before).total_seconds() >= 59


async def test_delayed_job_is_not_claimable_yet(manager: JobManager):
    @task
    async def send_reminder() -> None:
        pass

    await send_reminder.enqueue(delay=3600)

    assert await manager.backend.claim_job("worker-1", lease_buffer_seconds=30) is None


async def test_zero_delay_is_claimable_immediately(manager: JobManager):
    @task
    async def send_now() -> None:
        pass

    job = await send_now.enqueue()

    claimed = await manager.backend.claim_job("worker-1", lease_buffer_seconds=30)
    assert claimed is not None
    assert claimed.id == job.id
