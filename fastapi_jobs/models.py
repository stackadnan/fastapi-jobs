from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Literal


class JobStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    RETRYING = "RETRYING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


TERMINAL_STATUSES = frozenset({JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.CANCELLED})


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 1
    backoff: Literal["fixed", "exponential"] = "exponential"
    base_delay: float = 1.0
    max_delay: float = 300.0
    jitter: bool = True
    retry_on: tuple[type[BaseException], ...] = (Exception,)


@dataclass(frozen=True, slots=True)
class Job:
    """A read-only snapshot of a job's state at the time it was fetched.

    Statuses change as workers pick jobs up, so treat this as a point-in-time view --
    call the manager again to see whether anything has changed.
    """

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
    lease_expires_at: datetime | None
    locked_by: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    cancel_requested_at: datetime | None
