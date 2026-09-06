from __future__ import annotations

import asyncio
import time

from fastapi_jobs.decorators import task
from fastapi_jobs.manager import JobManager
from fastapi_jobs.models import JobStatus
from fastapi_jobs.worker import Worker


async def _wait_for_status(
    manager: JobManager, job_id: str, status: JobStatus, *, timeout: float = 2.0
):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = await manager.get(job_id)
        if job.status is status:
            return job
        await asyncio.sleep(0.02)
    raise AssertionError(f"job {job_id} never reached {status} within {timeout}s")


async def test_worker_cancels_a_running_async_job(manager: JobManager):
    started = asyncio.Event()

    @task
    async def blocks_forever() -> None:
        started.set()
        # Nothing ever sets this -- the only way out is cancellation.
        await asyncio.Event().wait()

    job = await blocks_forever.enqueue()
    worker = Worker(manager, poll_interval=0.02)
    run = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(started.wait(), timeout=2.0)

        requested = await manager.cancel(job.id)
        assert requested.status is JobStatus.RUNNING
        assert requested.cancel_requested_at is not None

        finished = await _wait_for_status(manager, job.id, JobStatus.CANCELLED)
        assert finished.finished_at is not None
    finally:
        worker.request_shutdown()
        await asyncio.wait_for(run, timeout=2.0)


async def test_cancelled_job_is_not_retried(manager: JobManager):
    started = asyncio.Event()

    @task(retries=5)
    async def blocks_forever() -> None:
        started.set()
        await asyncio.Event().wait()

    job = await blocks_forever.enqueue()
    worker = Worker(manager, poll_interval=0.02)
    run = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(started.wait(), timeout=2.0)
        await manager.cancel(job.id)

        finished = await _wait_for_status(manager, job.id, JobStatus.CANCELLED)
        # A retry would have bumped attempts and put it back in RETRYING; it must
        # have gone straight to CANCELLED after a single attempt instead.
        assert finished.attempts == 1
    finally:
        worker.request_shutdown()
        await asyncio.wait_for(run, timeout=2.0)


async def test_cancelling_one_job_does_not_affect_another(manager: JobManager):
    victim_started = asyncio.Event()
    survivor_done = asyncio.Event()

    @task(name="victim")
    async def victim() -> None:
        victim_started.set()
        await asyncio.Event().wait()

    @task(name="survivor")
    async def survivor() -> str:
        await victim_started.wait()
        survivor_done.set()
        return "ok"

    victim_job = await victim.enqueue()
    survivor_job = await survivor.enqueue()
    worker = Worker(manager, poll_interval=0.02, max_concurrency=2)
    run = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(victim_started.wait(), timeout=2.0)
        await manager.cancel(victim_job.id)

        await asyncio.wait_for(survivor_done.wait(), timeout=2.0)
        finished_survivor = await _wait_for_status(manager, survivor_job.id, JobStatus.SUCCESS)
        finished_victim = await _wait_for_status(manager, victim_job.id, JobStatus.CANCELLED)
    finally:
        worker.request_shutdown()
        await asyncio.wait_for(run, timeout=2.0)

    assert finished_survivor.result == "ok"
    assert finished_victim.status is JobStatus.CANCELLED


async def test_worker_keeps_processing_after_a_cancellation(manager: JobManager):
    started = asyncio.Event()

    @task(name="blocked")
    async def blocked() -> None:
        started.set()
        await asyncio.Event().wait()

    @task(name="after-cancel")
    async def after() -> str:
        return "done"

    blocked_job = await blocked.enqueue()
    worker = Worker(manager, poll_interval=0.02)
    run = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(started.wait(), timeout=2.0)
        await manager.cancel(blocked_job.id)
        await _wait_for_status(manager, blocked_job.id, JobStatus.CANCELLED)

        next_job = await after.enqueue()
        finished_next = await _wait_for_status(manager, next_job.id, JobStatus.SUCCESS)
        assert finished_next.result == "done"
    finally:
        worker.request_shutdown()
        await asyncio.wait_for(run, timeout=2.0)
