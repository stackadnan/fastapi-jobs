from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from fastapi_jobs.backends.base import JobBackend
from fastapi_jobs.config import JobsConfig
from fastapi_jobs.exceptions import (
    JobAlreadyCompletedError,
    JobNotFoundError,
    JobsNotConfiguredError,
)
from fastapi_jobs.models import TERMINAL_STATUSES, Job, JobStatus
from fastapi_jobs.serialization import encode_payload

if TYPE_CHECKING:
    from fastapi_jobs.decorators import Task


class JobManager:
    """The backend-agnostic surface Task.enqueue() and the FastAPI routes talk to."""

    def __init__(self, backend: JobBackend, config: JobsConfig) -> None:
        self.backend = backend
        self.config = config

    async def enqueue(
        self,
        task: Task,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        delay: float = 0.0,
    ) -> Job:
        payload = encode_payload(task.name, args, kwargs)
        timeout = task.timeout if task.timeout is not None else self.config.default_timeout
        available_at = datetime.now(UTC) + timedelta(seconds=delay)
        return await self.backend.create_job(
            task.name,
            payload,
            max_attempts=task.retry_policy.max_attempts,
            timeout_seconds=timeout,
            available_at=available_at,
        )

    async def get(self, job_id: str) -> Job:
        job = await self.backend.get_job(job_id)
        if job is None:
            raise JobNotFoundError(job_id)
        return job

    async def list(
        self,
        *,
        status: JobStatus | None = None,
        task_name: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Job]:
        return await self.backend.list_jobs(
            status=status, task_name=task_name, limit=limit, offset=offset
        )

    async def cancel(self, job_id: str) -> Job:
        """Cancel a job.

        A PENDING/RETRYING job is cancelled immediately. A RUNNING job instead has
        its cancellation flag set and is returned still RUNNING -- the worker holding
        its lease notices the flag, cancels its local task, and the job settles into
        CANCELLED once that unwinds. Call this again (or poll `get`) to observe that.
        """
        job = await self.get(job_id)
        if job.status in TERMINAL_STATUSES:
            raise JobAlreadyCompletedError(
                f"job '{job_id}' already reached a terminal state ({job.status.value})"
            )
        if job.status is JobStatus.RUNNING:
            await self.backend.request_cancellation(job_id)
            return await self.get(job_id)

        cancelled = await self.backend.cancel_job(job_id)
        if not cancelled:
            # Lost the race: a worker claimed the job between our read above and this
            # call reaching the backend. It's running now, so fall back to requesting
            # a cooperative cancellation instead of failing outright.
            await self.backend.request_cancellation(job_id)
        return await self.get(job_id)


_current_manager: JobManager | None = None


def set_current_manager(manager: JobManager | None) -> None:
    global _current_manager
    _current_manager = manager


def get_current_manager() -> JobManager:
    if _current_manager is None:
        raise JobsNotConfiguredError(
            "no active Jobs instance in this process; construct one with "
            "Jobs(app, database=...) before calling enqueue()"
        )
    return _current_manager
