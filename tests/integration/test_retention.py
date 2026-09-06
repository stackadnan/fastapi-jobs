from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi_jobs.backends.sqlite import SQLiteBackend
from fastapi_jobs.models import JobStatus


async def _create(backend: SQLiteBackend, *, max_attempts: int = 1):
    return await backend.create_job(
        "demo.task",
        '{"args": [], "kwargs": {}}',
        max_attempts=max_attempts,
        timeout_seconds=30,
        available_at=datetime.now(UTC),
    )


async def test_purge_removes_old_successful_jobs(backend: SQLiteBackend):
    job = await _create(backend)
    await backend.claim_job("worker-1", lease_buffer_seconds=30)
    await backend.complete_job(job.id, "worker-1", result=None)

    deleted = await backend.purge_jobs(older_than=timedelta(seconds=0))

    assert deleted == 1
    assert await backend.get_job(job.id) is None


async def test_purge_removes_old_failed_jobs(backend: SQLiteBackend):
    job = await _create(backend)
    await backend.claim_job("worker-1", lease_buffer_seconds=30)
    await backend.fail_job(job.id, "worker-1", error="boom")

    deleted = await backend.purge_jobs(older_than=timedelta(seconds=0))

    assert deleted == 1
    assert await backend.get_job(job.id) is None


async def test_purge_removes_old_cancelled_jobs(backend: SQLiteBackend):
    job = await _create(backend)
    assert await backend.cancel_job(job.id) is True

    deleted = await backend.purge_jobs(older_than=timedelta(seconds=0))

    assert deleted == 1
    assert await backend.get_job(job.id) is None


async def test_purge_leaves_recent_terminal_jobs_alone(backend: SQLiteBackend):
    job = await _create(backend)
    await backend.claim_job("worker-1", lease_buffer_seconds=30)
    await backend.complete_job(job.id, "worker-1", result=None)

    deleted = await backend.purge_jobs(older_than=timedelta(hours=1))

    assert deleted == 0
    assert await backend.get_job(job.id) is not None


async def test_purge_never_removes_pending_jobs(backend: SQLiteBackend):
    job = await _create(backend)

    deleted = await backend.purge_jobs(older_than=timedelta(seconds=0))

    assert deleted == 0
    assert await backend.get_job(job.id) is not None


async def test_purge_never_removes_running_jobs(backend: SQLiteBackend):
    job = await _create(backend)
    await backend.claim_job("worker-1", lease_buffer_seconds=30)

    deleted = await backend.purge_jobs(older_than=timedelta(seconds=0))

    result = await backend.get_job(job.id)
    assert deleted == 0
    assert result is not None
    assert result.status is JobStatus.RUNNING


async def test_purge_never_removes_retrying_jobs(backend: SQLiteBackend):
    job = await _create(backend, max_attempts=3)
    await backend.claim_job("worker-1", lease_buffer_seconds=30)
    await backend.schedule_retry(
        job.id, "worker-1", error="transient", available_at=datetime.now(UTC)
    )

    deleted = await backend.purge_jobs(older_than=timedelta(seconds=0))

    result = await backend.get_job(job.id)
    assert deleted == 0
    assert result is not None
    assert result.status is JobStatus.RETRYING


async def test_purge_never_removes_a_running_job_with_cancellation_requested(
    backend: SQLiteBackend,
):
    job = await _create(backend)
    await backend.claim_job("worker-1", lease_buffer_seconds=30)
    await backend.request_cancellation(job.id)

    deleted = await backend.purge_jobs(older_than=timedelta(seconds=0))

    result = await backend.get_job(job.id)
    assert deleted == 0
    assert result is not None
    assert result.status is JobStatus.RUNNING
