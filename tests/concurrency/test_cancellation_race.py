from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from fastapi_jobs.backends.sqlite import SQLiteBackend
from fastapi_jobs.exceptions import InvalidJobStateError
from fastapi_jobs.models import JobStatus


async def _create(backend: SQLiteBackend):
    return await backend.create_job(
        "demo.task",
        '{"args": [], "kwargs": {}}',
        max_attempts=1,
        timeout_seconds=30,
        available_at=datetime.now(UTC),
    )


async def test_request_cancellation_is_noop_after_job_already_succeeded(backend: SQLiteBackend):
    job = await _create(backend)
    await backend.claim_job("worker-1", lease_buffer_seconds=30)
    await backend.complete_job(job.id, "worker-1", result=None)

    requested = await backend.request_cancellation(job.id)

    assert requested is False
    done = await backend.get_job(job.id)
    assert done is not None
    assert done.status is JobStatus.SUCCESS
    assert done.cancel_requested_at is None


async def test_request_cancellation_is_idempotent(backend: SQLiteBackend):
    job = await _create(backend)
    await backend.claim_job("worker-1", lease_buffer_seconds=30)

    first = await backend.request_cancellation(job.id)
    second = await backend.request_cancellation(job.id)

    assert first is True
    assert second is True
    flagged = await backend.get_job(job.id)
    assert flagged is not None
    assert flagged.status is JobStatus.RUNNING
    assert flagged.cancel_requested_at is not None


async def test_finalize_cancellation_rejects_stale_lease(backend: SQLiteBackend):
    job = await backend.create_job(
        "demo.task",
        '{"args": [], "kwargs": {}}',
        max_attempts=1,
        timeout_seconds=0.05,
        available_at=datetime.now(UTC),
    )
    await backend.claim_job("worker-1", lease_buffer_seconds=0.05)
    await asyncio.sleep(0.2)
    reclaimed = await backend.claim_job("worker-2", lease_buffer_seconds=30)
    assert reclaimed is not None

    # worker-1 was cancelled locally after its lease already expired and was
    # reclaimed. Its finalize call must not be allowed to clobber worker-2.
    with pytest.raises(InvalidJobStateError):
        await backend.finalize_cancellation(job.id, "worker-1")

    still_running = await backend.get_job(job.id)
    assert still_running is not None
    assert still_running.status is JobStatus.RUNNING
    assert still_running.locked_by == "worker-2"


async def test_finalize_cancellation_succeeds_for_the_owning_worker(backend: SQLiteBackend):
    job = await _create(backend)
    await backend.claim_job("worker-1", lease_buffer_seconds=30)
    await backend.request_cancellation(job.id)

    await backend.finalize_cancellation(job.id, "worker-1")

    cancelled = await backend.get_job(job.id)
    assert cancelled is not None
    assert cancelled.status is JobStatus.CANCELLED
    assert cancelled.finished_at is not None
    assert cancelled.locked_by is None
