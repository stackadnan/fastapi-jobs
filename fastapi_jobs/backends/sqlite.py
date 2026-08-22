from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import aiosqlite

from fastapi_jobs.exceptions import InvalidJobStateError, JobNotFoundError
from fastapi_jobs.models import Job, JobStatus
from fastapi_jobs.serialization import decode_payload, decode_result

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id               TEXT PRIMARY KEY,
    task_name        TEXT NOT NULL,
    status           TEXT NOT NULL,
    payload          TEXT NOT NULL,
    result           TEXT,
    error            TEXT,
    attempts         INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL,
    timeout_seconds  REAL NOT NULL,
    available_at     TEXT NOT NULL,
    lease_expires_at TEXT,
    locked_by        TEXT,
    created_at       TEXT NOT NULL,
    started_at       TEXT,
    finished_at      TEXT
);
CREATE INDEX IF NOT EXISTS idx_jobs_claim ON jobs (status, available_at);
CREATE INDEX IF NOT EXISTS idx_jobs_task_name ON jobs (task_name);
"""

_CLAIMABLE_STATUSES = (JobStatus.PENDING.value, JobStatus.RETRYING.value)


def _parse_sqlite_url(url: str) -> str:
    prefix = "sqlite:///"
    if not url.startswith(prefix):
        raise ValueError(f"unsupported database url: '{url}'; expected a sqlite:/// url")
    # sqlite:///relative.db -> "relative.db", sqlite:////abs/path.db -> "/abs/path.db",
    # sqlite:///:memory: -> ":memory:". Slicing off just the scheme+separator leaves the
    # right thing in all three cases without special-casing any of them.
    return url[len(prefix) :] or ":memory:"


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat()


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _parse_iso_opt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


class SQLiteBackend:
    def __init__(self, database: str) -> None:
        self._path = _parse_sqlite_url(database)
        self._init_lock = asyncio.Lock()
        self._initialized = False

    async def initialize(self) -> None:
        if self._initialized:
            return
        async with self._init_lock:
            if self._initialized:
                return
            async with self._connection() as conn:
                await conn.executescript(_SCHEMA)
            self._initialized = True

    @asynccontextmanager
    async def _connection(self) -> AsyncIterator[aiosqlite.Connection]:
        # A fresh connection per operation keeps this backend simple and correct under
        # concurrent access -- SQLite's own file locking, plus busy_timeout below, does
        # the serialization for us. A shared/pooled connection would only pay for
        # itself at a throughput this backend was never meant for; see the SQLite
        # section of the README for the concurrency envelope this is designed around.
        conn = await aiosqlite.connect(self._path, isolation_level=None)
        conn.row_factory = aiosqlite.Row
        try:
            await conn.execute("PRAGMA busy_timeout = 5000")
            await conn.execute("PRAGMA journal_mode = WAL")
            yield conn
        finally:
            await conn.close()

    async def create_job(
        self,
        task_name: str,
        payload: str,
        *,
        max_attempts: int,
        timeout_seconds: float,
        available_at: datetime,
    ) -> Job:
        await self.initialize()
        job_id = uuid.uuid4().hex
        now = datetime.now(UTC)
        async with self._connection() as conn:
            await conn.execute(
                """
                INSERT INTO jobs
                    (id, task_name, status, payload, attempts, max_attempts,
                     timeout_seconds, available_at, created_at)
                VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    task_name,
                    JobStatus.PENDING.value,
                    payload,
                    max_attempts,
                    timeout_seconds,
                    _iso(available_at),
                    _iso(now),
                ),
            )
        job = await self.get_job(job_id)
        assert job is not None  # nothing else can delete a row we just created
        return job

    async def get_job(self, job_id: str) -> Job | None:
        await self.initialize()
        async with self._connection() as conn:
            cursor = await conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
            row = await cursor.fetchone()
        return self._row_to_job(row) if row else None

    async def claim_job(self, worker_id: str, *, lease_buffer_seconds: float) -> Job | None:
        await self.initialize()
        now = datetime.now(UTC)
        async with self._connection() as conn:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                cursor = await conn.execute(
                    """
                    SELECT id, status, timeout_seconds FROM jobs
                    WHERE (status IN (?, ?) AND available_at <= ?)
                       OR (status = ? AND lease_expires_at < ?)
                    ORDER BY available_at
                    LIMIT 1
                    """,
                    (*_CLAIMABLE_STATUSES, _iso(now), JobStatus.RUNNING.value, _iso(now)),
                )
                row = await cursor.fetchone()
                if row is None:
                    await conn.execute("ROLLBACK")
                    return None

                job_id, prior_status, timeout_seconds = row
                lease_expires_at = now + timedelta(seconds=timeout_seconds + lease_buffer_seconds)
                await conn.execute(
                    """
                    UPDATE jobs
                    SET status = ?, locked_by = ?, lease_expires_at = ?,
                        started_at = COALESCE(started_at, ?), attempts = attempts + 1
                    WHERE id = ? AND status = ?
                    """,
                    (
                        JobStatus.RUNNING.value,
                        worker_id,
                        _iso(lease_expires_at),
                        _iso(now),
                        job_id,
                        prior_status,
                    ),
                )
                cursor = await conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
                claimed = await cursor.fetchone()
                await conn.execute("COMMIT")
            except BaseException:
                await conn.execute("ROLLBACK")
                raise
        return self._row_to_job(claimed) if claimed else None

    async def complete_job(self, job_id: str, worker_id: str, *, result: str | None) -> None:
        await self.initialize()
        now = _iso(datetime.now(UTC))
        async with self._connection() as conn:
            cursor = await conn.execute(
                """
                UPDATE jobs
                SET status = ?, result = ?, finished_at = ?,
                    lease_expires_at = NULL, locked_by = NULL
                WHERE id = ? AND status = ? AND locked_by = ?
                """,
                (JobStatus.SUCCESS.value, result, now, job_id, JobStatus.RUNNING.value, worker_id),
            )
        if cursor.rowcount == 0:
            await self._raise_stale_write(job_id)

    async def schedule_retry(
        self, job_id: str, worker_id: str, *, error: str, available_at: datetime
    ) -> None:
        await self.initialize()
        async with self._connection() as conn:
            cursor = await conn.execute(
                """
                UPDATE jobs
                SET status = ?, error = ?, available_at = ?,
                    lease_expires_at = NULL, locked_by = NULL
                WHERE id = ? AND status = ? AND locked_by = ?
                """,
                (
                    JobStatus.RETRYING.value,
                    error,
                    _iso(available_at),
                    job_id,
                    JobStatus.RUNNING.value,
                    worker_id,
                ),
            )
        if cursor.rowcount == 0:
            await self._raise_stale_write(job_id)

    async def fail_job(self, job_id: str, worker_id: str, *, error: str) -> None:
        await self.initialize()
        now = _iso(datetime.now(UTC))
        async with self._connection() as conn:
            cursor = await conn.execute(
                """
                UPDATE jobs
                SET status = ?, error = ?, finished_at = ?,
                    lease_expires_at = NULL, locked_by = NULL
                WHERE id = ? AND status = ? AND locked_by = ?
                """,
                (JobStatus.FAILED.value, error, now, job_id, JobStatus.RUNNING.value, worker_id),
            )
        if cursor.rowcount == 0:
            await self._raise_stale_write(job_id)

    async def cancel_job(self, job_id: str) -> bool:
        await self.initialize()
        now = _iso(datetime.now(UTC))
        async with self._connection() as conn:
            cursor = await conn.execute(
                """
                UPDATE jobs SET status = ?, finished_at = ?
                WHERE id = ? AND status IN (?, ?)
                """,
                (
                    JobStatus.CANCELLED.value,
                    now,
                    job_id,
                    JobStatus.PENDING.value,
                    JobStatus.RETRYING.value,
                ),
            )
        return cursor.rowcount > 0

    async def list_jobs(
        self,
        *,
        status: JobStatus | None = None,
        task_name: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Job]:
        await self.initialize()
        clauses = []
        params: list[Any] = []
        if status is not None:
            clauses.append("status = ?")
            params.append(status.value)
        if task_name is not None:
            clauses.append("task_name = ?")
            params.append(task_name)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.extend([limit, offset])
        async with self._connection() as conn:
            cursor = await conn.execute(
                f"SELECT * FROM jobs {where} ORDER BY created_at DESC LIMIT ? OFFSET ?",
                params,
            )
            rows = await cursor.fetchall()
        return [self._row_to_job(row) for row in rows]

    async def _raise_stale_write(self, job_id: str) -> None:
        job = await self.get_job(job_id)
        if job is None:
            raise JobNotFoundError(job_id)
        # The row exists but our CAS (status='RUNNING' AND locked_by=<us>) didn't
        # match, meaning our lease had already expired and another worker reclaimed
        # the job before we finished. Writing our outcome now would clobber whatever
        # that newer attempt is doing, so we discard it instead.
        raise InvalidJobStateError(
            f"job '{job_id}' was reclaimed by another worker before this attempt finished"
        )

    def _row_to_job(self, row: aiosqlite.Row) -> Job:
        args, kwargs = decode_payload(row["payload"])
        return Job(
            id=row["id"],
            task_name=row["task_name"],
            status=JobStatus(row["status"]),
            args=args,
            kwargs=kwargs,
            result=decode_result(row["result"]),
            error=row["error"],
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            timeout_seconds=row["timeout_seconds"],
            available_at=_parse_iso(row["available_at"]),
            lease_expires_at=_parse_iso_opt(row["lease_expires_at"]),
            locked_by=row["locked_by"],
            created_at=_parse_iso(row["created_at"]),
            started_at=_parse_iso_opt(row["started_at"]),
            finished_at=_parse_iso_opt(row["finished_at"]),
        )
