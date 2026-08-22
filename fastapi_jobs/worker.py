from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import random
import signal
import socket
import time
import traceback
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

from fastapi_jobs.decorators import get_task
from fastapi_jobs.executor import run_task
from fastapi_jobs.manager import JobManager
from fastapi_jobs.models import Job, RetryPolicy
from fastapi_jobs.serialization import encode_result

if TYPE_CHECKING:
    from fastapi_jobs.decorators import Task

logger = logging.getLogger("fastapi_jobs.worker")


def compute_backoff(attempt: int, policy: RetryPolicy) -> float:
    if policy.backoff == "fixed":
        delay = policy.base_delay
    else:
        delay = policy.base_delay * 2 ** (attempt - 1)
    delay = min(delay, policy.max_delay)
    return random.uniform(0, delay) if policy.jitter else delay


def _format_error(exc: BaseException) -> str:
    return "".join(traceback.format_exception(type(exc), exc, exc.__traceback__, limit=10))


class Worker:
    """Polls the backend for claimable jobs and runs them until asked to stop.

    Execution is at-least-once, not exactly-once: a worker that crashes after a job
    finishes but before it records SUCCESS will have that job reclaimed and retried
    once its lease expires. Tasks with external side effects should be written to
    tolerate being run more than once for the same job.
    """

    def __init__(
        self,
        manager: JobManager,
        *,
        worker_id: str | None = None,
        poll_interval: float | None = None,
    ) -> None:
        self.manager = manager
        self.worker_id = worker_id or f"{socket.gethostname()}-{os.getpid()}-{uuid4().hex[:8]}"
        self.poll_interval = (
            poll_interval if poll_interval is not None else manager.config.poll_interval
        )
        self._shutdown = asyncio.Event()

    def request_shutdown(self) -> None:
        self._shutdown.set()

    async def run(self) -> None:
        self._install_signal_handlers()
        await self.manager.backend.initialize()
        logger.info("worker started", extra={"worker_id": self.worker_id})
        try:
            while not self._shutdown.is_set():
                job = await self.manager.backend.claim_job(
                    self.worker_id, lease_buffer_seconds=self.manager.config.lease_buffer
                )
                if job is None:
                    await self._wait_or_shutdown(self.poll_interval)
                    continue
                await self._execute(job)
        finally:
            logger.info("worker stopped", extra={"worker_id": self.worker_id})

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self.request_shutdown)
            except NotImplementedError:
                # add_signal_handler isn't implemented on Windows' proactor loop.
                signal.signal(sig, lambda *_: self.request_shutdown())

    async def _wait_or_shutdown(self, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._shutdown.wait(), timeout=seconds)

    async def _execute(self, job: Job) -> None:
        task = get_task(job.task_name)
        if task is None:
            logger.error(
                "no task registered for job, failing it",
                extra={"job_id": job.id, "task_name": job.task_name},
            )
            await self.manager.backend.fail_job(
                job.id, self.worker_id, error=f"task '{job.task_name}' is not registered"
            )
            return

        started = time.monotonic()
        try:
            result = await run_task(task, job)
        except Exception as exc:
            # Deliberately broad: a task's own exception must never crash the worker
            # loop. It's captured and recorded below, not swallowed.
            await self._handle_failure(job, task, exc)
            return

        duration = time.monotonic() - started
        encoded = encode_result(task.name, result)
        await self.manager.backend.complete_job(job.id, self.worker_id, result=encoded)
        logger.info(
            "job succeeded",
            extra={
                "job_id": job.id,
                "task_name": job.task_name,
                "attempt": job.attempts,
                "duration": round(duration, 3),
            },
        )

    async def _handle_failure(self, job: Job, task: Task, exc: Exception) -> None:
        retryable = isinstance(exc, task.retry_policy.retry_on)
        error = _format_error(exc)
        if retryable and job.attempts < job.max_attempts:
            delay = compute_backoff(job.attempts, task.retry_policy)
            available_at = datetime.now(UTC) + timedelta(seconds=delay)
            await self.manager.backend.schedule_retry(
                job.id, self.worker_id, error=error, available_at=available_at
            )
            logger.warning(
                "job failed, scheduled for retry",
                extra={
                    "job_id": job.id,
                    "task_name": job.task_name,
                    "attempt": job.attempts,
                    "delay": round(delay, 3),
                },
            )
        else:
            await self.manager.backend.fail_job(job.id, self.worker_id, error=error)
            logger.error(
                "job failed permanently",
                extra={"job_id": job.id, "task_name": job.task_name, "attempt": job.attempts},
            )
