from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from fastapi_jobs.backends.sqlite import SQLiteBackend


async def test_only_one_worker_claims_a_single_job(backend: SQLiteBackend):
    job = await backend.create_job(
        "demo.task",
        '{"args": [], "kwargs": {}}',
        max_attempts=1,
        timeout_seconds=30,
        available_at=datetime.now(UTC),
    )

    results = await asyncio.gather(
        *(backend.claim_job(f"worker-{i}", lease_buffer_seconds=30) for i in range(10))
    )

    claimed = [r for r in results if r is not None]
    assert len(claimed) == 1
    assert claimed[0].id == job.id
    assert claimed[0].attempts == 1


async def test_ten_workers_racing_for_ten_jobs_each_get_exactly_one(backend: SQLiteBackend):
    jobs = [
        await backend.create_job(
            "demo.task",
            '{"args": [], "kwargs": {}}',
            max_attempts=1,
            timeout_seconds=30,
            available_at=datetime.now(UTC),
        )
        for _ in range(10)
    ]

    results = await asyncio.gather(
        *(backend.claim_job(f"worker-{i}", lease_buffer_seconds=30) for i in range(10))
    )

    claimed_ids = {r.id for r in results if r is not None}
    assert claimed_ids == {j.id for j in jobs}
    assert len(results) == len(set(r.id for r in results if r is not None))


async def test_concurrent_job_creation_produces_distinct_ids(backend: SQLiteBackend):
    created = await asyncio.gather(
        *(
            backend.create_job(
                "demo.task",
                '{"args": [], "kwargs": {}}',
                max_attempts=1,
                timeout_seconds=30,
                available_at=datetime.now(UTC),
            )
            for _ in range(20)
        )
    )
    assert len({job.id for job in created}) == 20
