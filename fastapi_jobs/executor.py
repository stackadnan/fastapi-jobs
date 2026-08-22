from __future__ import annotations

import asyncio
import inspect
from typing import TYPE_CHECKING, Any

from fastapi_jobs.exceptions import JobTimeoutError
from fastapi_jobs.models import Job

if TYPE_CHECKING:
    from fastapi_jobs.decorators import Task


async def run_task(task: Task, job: Job) -> Any:
    if inspect.iscoroutinefunction(task.func):
        awaitable = task.func(*job.args, **job.kwargs)
    else:
        # Sync tasks run in a thread so they don't block the worker's event loop and
        # starve every other job it's polling for. wait_for below can only stop
        # *waiting* on that thread, not the thread itself -- a sync task that hangs
        # keeps running in the background after we've already marked the job timed out.
        awaitable = asyncio.to_thread(task.func, *job.args, **job.kwargs)
    try:
        return await asyncio.wait_for(awaitable, timeout=job.timeout_seconds)
    except TimeoutError as exc:
        raise JobTimeoutError(
            f"task '{task.name}' exceeded its {job.timeout_seconds}s timeout"
        ) from exc
