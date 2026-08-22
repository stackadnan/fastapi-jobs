from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from fastapi_jobs.backends.sqlite import SQLiteBackend
from fastapi_jobs.exceptions import InvalidJobStateError
from fastapi_jobs.models import JobStatus


async def test_expired_lease_is_reclaimed_by_another_worker(backend: SQLiteBackend):
    job = await backend.create_job(
        "demo.task",
        '{"args": [], "kwargs": {}}',
        max_attempts=5,
        timeout_seconds=0.05,
        available_at=datetime.now(UTC),
    )

    first = await backend.claim_job("worker-1", lease_buffer_seconds=0.05)
    assert first is not None
    assert first.status is JobStatus.RUNNING

    # worker-1 "crashed" -- it never calls complete_job/fail_job. Once
    # timeout_seconds + lease_buffer_seconds has elapsed, the row looks exactly like
    # an in-flight job whose worker died, which is the case this is standing in for.
    await asyncio.sleep(0.2)

    second = await backend.claim_job("worker-2", lease_buffer_seconds=0.05)
    assert second is not None
    assert second.id == job.id
    assert second.locked_by == "worker-2"
    assert second.attempts == 2  # claimed twice: once by worker-1, once by worker-2


async def test_late_write_from_the_original_worker_is_rejected(backend: SQLiteBackend):
    job = await backend.create_job(
        "demo.task",
        '{"args": [], "kwargs": {}}',
        max_attempts=5,
        timeout_seconds=0.05,
        available_at=datetime.now(UTC),
    )
    await backend.claim_job("worker-1", lease_buffer_seconds=0.05)
    await asyncio.sleep(0.2)
    reclaimed = await backend.claim_job("worker-2", lease_buffer_seconds=30)
    assert reclaimed is not None

    # worker-1 finally wakes up and tries to report success on a job it no longer
    # owns. That must not be allowed to clobber worker-2's in-progress attempt.
    with pytest.raises(InvalidJobStateError):
        await backend.complete_job(job.id, "worker-1", result=None)

    still_running = await backend.get_job(job.id)
    assert still_running is not None
    assert still_running.status is JobStatus.RUNNING
    assert still_running.locked_by == "worker-2"


async def test_lease_not_reclaimed_before_it_expires(backend: SQLiteBackend):
    await backend.create_job(
        "demo.task",
        '{"args": [], "kwargs": {}}',
        max_attempts=5,
        timeout_seconds=30,
        available_at=datetime.now(UTC),
    )
    first = await backend.claim_job("worker-1", lease_buffer_seconds=30)
    assert first is not None

    second = await backend.claim_job("worker-2", lease_buffer_seconds=30)
    assert second is None
