from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from fastapi_jobs.exceptions import InvalidJobStateError, JobNotFoundError
from fastapi_jobs.manager import JobManager
from fastapi_jobs.models import Job, JobStatus


@dataclass(frozen=True)
class ApiConfig:
    """Controls the optional REST API mounted by `Jobs(app, api=...)`.

    Nothing is mounted unless `api` is passed, and mounting it makes job arguments and
    results readable to whoever can reach these routes -- put something in
    `dependencies` (e.g. `[Depends(require_admin)]`) unless the app is already
    otherwise gated. There's no endpoint to create or re-run a job: exposing one that
    lets a caller name any registered task and pass it arbitrary arguments would turn
    this into a task-injection vector, so job creation stays code-only via .enqueue().
    """

    prefix: str = "/jobs"
    tags: Sequence[str] = field(default_factory=lambda: ["jobs"])
    dependencies: Sequence[Any] = field(default_factory=tuple)
    default_limit: int = 50
    max_limit: int = 200


class JobOut(BaseModel):
    id: str
    task_name: str
    status: JobStatus
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    result: Any
    error: str | None
    attempts: int
    max_attempts: int
    timeout_seconds: float
    available_at: datetime
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    cancel_requested_at: datetime | None

    @classmethod
    def from_job(cls, job: Job) -> JobOut:
        return cls(
            id=job.id,
            task_name=job.task_name,
            status=job.status,
            args=job.args,
            kwargs=job.kwargs,
            result=job.result,
            # The full traceback is kept internally (Job.error) but isn't put on the
            # wire by default -- it can reveal source paths and internals to whoever
            # can reach this endpoint.
            error=job.error.splitlines()[0] if job.error else None,
            attempts=job.attempts,
            max_attempts=job.max_attempts,
            timeout_seconds=job.timeout_seconds,
            available_at=job.available_at,
            created_at=job.created_at,
            started_at=job.started_at,
            finished_at=job.finished_at,
            cancel_requested_at=job.cancel_requested_at,
        )


def build_router(manager: JobManager, config: ApiConfig) -> APIRouter:
    router = APIRouter(
        prefix=config.prefix, tags=list(config.tags), dependencies=list(config.dependencies)
    )

    @router.get("/{job_id}", response_model=JobOut)
    async def get_job(job_id: str) -> JobOut:
        try:
            job = await manager.get(job_id)
        except JobNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return JobOut.from_job(job)

    @router.get("", response_model=list[JobOut])
    async def list_jobs(
        status: JobStatus | None = None,
        task_name: str | None = None,
        limit: int = Query(config.default_limit, ge=1, le=config.max_limit),
        offset: int = Query(0, ge=0),
    ) -> list[JobOut]:
        jobs = await manager.list(status=status, task_name=task_name, limit=limit, offset=offset)
        return [JobOut.from_job(job) for job in jobs]

    @router.post("/{job_id}/cancel", response_model=JobOut)
    async def cancel_job(job_id: str) -> JobOut:
        try:
            job = await manager.cancel(job_id)
        except JobNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except InvalidJobStateError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return JobOut.from_job(job)

    return router
