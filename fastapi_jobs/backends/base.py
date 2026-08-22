from __future__ import annotations

from datetime import datetime
from typing import Protocol

from fastapi_jobs.models import Job, JobStatus


class JobBackend(Protocol):
    """Storage operations a job backend must provide.

    These map to what actually happens to a job over its lifetime rather than to
    generic CRUD, because the interesting part -- claiming a job without letting two
    workers run it at once, and not letting a worker whose lease already expired
    clobber whatever claimed it next -- has to be a property of the storage layer
    itself, not something bolted on above it.
    """

    async def initialize(self) -> None: ...

    async def create_job(
        self,
        task_name: str,
        payload: str,
        *,
        max_attempts: int,
        timeout_seconds: float,
        available_at: datetime,
    ) -> Job: ...

    async def get_job(self, job_id: str) -> Job | None: ...

    async def claim_job(self, worker_id: str, *, lease_buffer_seconds: float) -> Job | None: ...

    async def complete_job(self, job_id: str, worker_id: str, *, result: str | None) -> None: ...

    async def schedule_retry(
        self, job_id: str, worker_id: str, *, error: str, available_at: datetime
    ) -> None: ...

    async def fail_job(self, job_id: str, worker_id: str, *, error: str) -> None: ...

    async def cancel_job(self, job_id: str) -> bool: ...

    async def list_jobs(
        self,
        *,
        status: JobStatus | None = None,
        task_name: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Job]: ...
