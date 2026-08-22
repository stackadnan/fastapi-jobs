from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from fastapi_jobs.backends.sqlite import SQLiteBackend
from fastapi_jobs.exceptions import InvalidJobStateError, JobNotFoundError
from fastapi_jobs.models import JobStatus


async def _create(backend: SQLiteBackend, *, max_attempts: int = 1, delay: float = 0.0):
    return await backend.create_job(
        "demo.task",
        '{"args": [1], "kwargs": {}}',
        max_attempts=max_attempts,
        timeout_seconds=30,
        available_at=datetime.now(UTC) + timedelta(seconds=delay),
    )


async def test_create_and_get_job(backend: SQLiteBackend):
    created = await _create(backend)
    fetched = await backend.get_job(created.id)
    assert fetched is not None
    assert fetched.status is JobStatus.PENDING
    assert fetched.args == (1,)


async def test_get_missing_job_returns_none(backend: SQLiteBackend):
    assert await backend.get_job("does-not-exist") is None


async def test_claim_marks_running_and_increments_attempts(backend: SQLiteBackend):
    job = await _create(backend)
    claimed = await backend.claim_job("worker-1", lease_buffer_seconds=30)
    assert claimed is not None
    assert claimed.id == job.id
    assert claimed.status is JobStatus.RUNNING
    assert claimed.attempts == 1
    assert claimed.locked_by == "worker-1"


async def test_claim_skips_future_jobs(backend: SQLiteBackend):
    await _create(backend, delay=3600)
    assert await backend.claim_job("worker-1", lease_buffer_seconds=30) is None


async def test_claim_returns_none_when_nothing_available(backend: SQLiteBackend):
    assert await backend.claim_job("worker-1", lease_buffer_seconds=30) is None


async def test_complete_job(backend: SQLiteBackend):
    job = await _create(backend)
    claimed = await backend.claim_job("worker-1", lease_buffer_seconds=30)
    assert claimed is not None
    await backend.complete_job(job.id, "worker-1", result='{"ok": true}')
    done = await backend.get_job(job.id)
    assert done is not None
    assert done.status is JobStatus.SUCCESS
    assert done.result == {"ok": True}
    assert done.finished_at is not None
    assert done.locked_by is None


async def test_fail_job_without_retries_is_terminal(backend: SQLiteBackend):
    job = await _create(backend, max_attempts=1)
    await backend.claim_job("worker-1", lease_buffer_seconds=30)
    await backend.fail_job(job.id, "worker-1", error="boom")
    failed = await backend.get_job(job.id)
    assert failed is not None
    assert failed.status is JobStatus.FAILED
    assert failed.error == "boom"


async def test_schedule_retry_makes_job_claimable_again(backend: SQLiteBackend):
    job = await _create(backend, max_attempts=3)
    await backend.claim_job("worker-1", lease_buffer_seconds=30)
    await backend.schedule_retry(
        job.id, "worker-1", error="transient", available_at=datetime.now(UTC)
    )
    retrying = await backend.get_job(job.id)
    assert retrying is not None
    assert retrying.status is JobStatus.RETRYING

    claimed_again = await backend.claim_job("worker-2", lease_buffer_seconds=30)
    assert claimed_again is not None
    assert claimed_again.id == job.id
    assert claimed_again.attempts == 2


async def test_complete_job_rejects_stale_lease(backend: SQLiteBackend):
    job = await _create(backend)
    await backend.claim_job("worker-1", lease_buffer_seconds=30)
    # worker-2 never actually claimed it -- simulates a worker whose write arrives
    # after it already lost the job (e.g. its lease expired and someone else took it).
    with pytest.raises(InvalidJobStateError):
        await backend.complete_job(job.id, "worker-2", result=None)


async def test_complete_job_missing_raises_not_found(backend: SQLiteBackend):
    with pytest.raises(JobNotFoundError):
        await backend.complete_job("missing", "worker-1", result=None)


async def test_cancel_pending_job(backend: SQLiteBackend):
    job = await _create(backend)
    assert await backend.cancel_job(job.id) is True
    cancelled = await backend.get_job(job.id)
    assert cancelled is not None
    assert cancelled.status is JobStatus.CANCELLED


async def test_cancel_running_job_fails(backend: SQLiteBackend):
    job = await _create(backend)
    await backend.claim_job("worker-1", lease_buffer_seconds=30)
    assert await backend.cancel_job(job.id) is False


async def test_list_jobs_filters_by_status(backend: SQLiteBackend):
    a = await _create(backend)
    b = await _create(backend)
    await backend.claim_job("worker-1", lease_buffer_seconds=30)

    pending = await backend.list_jobs(status=JobStatus.PENDING)
    running = await backend.list_jobs(status=JobStatus.RUNNING)

    pending_ids = {j.id for j in pending}
    running_ids = {j.id for j in running}
    assert pending_ids | running_ids == {a.id, b.id}
    assert len(running) == 1
