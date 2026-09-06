from __future__ import annotations

import asyncio
import time

from fastapi_jobs.decorators import task
from fastapi_jobs.manager import JobManager
from fastapi_jobs.models import JobStatus
from fastapi_jobs.worker import Worker


async def _wait_for_terminal(manager: JobManager, job_id: str, *, timeout: float = 2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = await manager.get(job_id)
        if job.status in (JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.CANCELLED):
            return job
        await asyncio.sleep(0.02)
    raise AssertionError(f"job {job_id} never reached a terminal state within {timeout}s")


async def test_max_concurrency_one_runs_jobs_sequentially(manager: JobManager):
    order: list[str] = []
    release_a = asyncio.Event()

    @task(name="seq-a")
    async def job_a() -> None:
        order.append("start:a")
        await release_a.wait()
        order.append("end:a")

    @task(name="seq-b")
    async def job_b() -> None:
        order.append("start:b")
        order.append("end:b")

    a = await job_a.enqueue()
    b = await job_b.enqueue()
    worker = Worker(manager, poll_interval=0.02, max_concurrency=1)
    run = asyncio.create_task(worker.run())
    try:
        # While job_a holds the single slot, job_b must not have started yet.
        await asyncio.sleep(0.1)
        assert order == ["start:a"]

        release_a.set()
        await _wait_for_terminal(manager, a.id)
        await _wait_for_terminal(manager, b.id)
    finally:
        worker.request_shutdown()
        await asyncio.wait_for(run, timeout=2.0)

    assert order == ["start:a", "end:a", "start:b", "end:b"]


async def test_max_concurrency_two_runs_jobs_at_the_same_time(manager: JobManager):
    both_entered = asyncio.Event()
    release = asyncio.Event()
    entered = 0

    @task
    async def slow(name: str) -> str:
        nonlocal entered
        entered += 1
        if entered == 2:
            both_entered.set()
        await release.wait()
        return name

    a = await slow.enqueue("a")
    b = await slow.enqueue("b")
    worker = Worker(manager, poll_interval=0.02, max_concurrency=2)
    run = asyncio.create_task(worker.run())
    try:
        # Only satisfiable if both jobs were claimed and started before either
        # finished -- proves real concurrency, not fast sequential execution.
        await asyncio.wait_for(both_entered.wait(), timeout=2.0)
        release.set()

        finished_a = await _wait_for_terminal(manager, a.id)
        finished_b = await _wait_for_terminal(manager, b.id)
    finally:
        worker.request_shutdown()
        await asyncio.wait_for(run, timeout=2.0)

    assert finished_a.status is JobStatus.SUCCESS
    assert finished_b.status is JobStatus.SUCCESS


async def test_max_concurrency_is_never_exceeded(manager: JobManager):
    current = 0
    reached_limit = asyncio.Event()
    release = asyncio.Event()

    @task
    async def slow(name: str) -> None:
        nonlocal current
        current += 1
        try:
            # A hard invariant checked from inside the task itself: if the worker
            # ever over-schedules, this fails the job rather than relying on timing.
            assert current <= 2
            if current == 2:
                reached_limit.set()
            await release.wait()
        finally:
            current -= 1

    jobs = [await slow.enqueue(f"job-{i}") for i in range(5)]
    worker = Worker(manager, poll_interval=0.02, max_concurrency=2)
    run = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(reached_limit.wait(), timeout=2.0)
        # Give a hypothetical over-scheduled third task a moment to start before
        # releasing everything.
        await asyncio.sleep(0.05)
        release.set()
        finished = [await _wait_for_terminal(manager, job.id) for job in jobs]
    finally:
        worker.request_shutdown()
        await asyncio.wait_for(run, timeout=2.0)

    assert all(job.status is JobStatus.SUCCESS for job in finished)


async def test_slow_job_does_not_block_other_queued_jobs(manager: JobManager):
    slow_started = asyncio.Event()
    slow_release = asyncio.Event()

    @task(name="slow-blocker")
    async def slow() -> None:
        slow_started.set()
        await slow_release.wait()

    @task(name="fast-job")
    async def fast() -> str:
        return "done"

    slow_job = await slow.enqueue()
    fast_job = await fast.enqueue()
    worker = Worker(manager, poll_interval=0.02, max_concurrency=2)
    run = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(slow_started.wait(), timeout=2.0)
        finished_fast = await _wait_for_terminal(manager, fast_job.id)
        assert finished_fast.status is JobStatus.SUCCESS

        still_running = await manager.get(slow_job.id)
        assert still_running.status is JobStatus.RUNNING
    finally:
        slow_release.set()
        worker.request_shutdown()
        await asyncio.wait_for(run, timeout=2.0)


async def test_shutdown_waits_for_multiple_running_jobs(manager: JobManager):
    entered = 0
    all_entered = asyncio.Event()
    release = asyncio.Event()

    @task
    async def slow(name: str) -> str:
        nonlocal entered
        entered += 1
        if entered == 3:
            all_entered.set()
        await release.wait()
        return name

    jobs = [await slow.enqueue(f"job-{i}") for i in range(3)]
    worker = Worker(manager, poll_interval=0.02, max_concurrency=3)
    run = asyncio.create_task(worker.run())

    await asyncio.wait_for(all_entered.wait(), timeout=2.0)
    worker.request_shutdown()

    # Shutdown must not cut these off early -- they're still RUNNING until released.
    await asyncio.sleep(0.05)
    for job in jobs:
        assert (await manager.get(job.id)).status is JobStatus.RUNNING

    release.set()
    await asyncio.wait_for(run, timeout=2.0)

    for job in jobs:
        finished = await manager.get(job.id)
        assert finished.status is JobStatus.SUCCESS
