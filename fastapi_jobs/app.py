from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi_jobs.backends.base import JobBackend
from fastapi_jobs.backends.sqlite import SQLiteBackend
from fastapi_jobs.config import JobsConfig
from fastapi_jobs.manager import JobManager, set_current_manager
from fastapi_jobs.router import ApiConfig, build_router

if TYPE_CHECKING:
    from fastapi import FastAPI


def _build_backend(config: JobsConfig) -> JobBackend:
    if config.database.startswith("sqlite://"):
        return SQLiteBackend(config.database)
    raise ValueError(
        f"unsupported database url: '{config.database}'; only sqlite:// is supported "
        "by this version of fastapi-jobs"
    )


class Jobs:
    """Wires a job backend to the process and, once mounted, to a FastAPI app.

    Only one Jobs instance is expected to be active per process -- @task-decorated
    functions look it up implicitly when you call .enqueue(). Constructing a second
    one replaces the first for that purpose, which is mainly useful in tests.
    """

    def __init__(
        self,
        app: FastAPI | None = None,
        *,
        database: str = "sqlite:///jobs.db",
        default_timeout: float = 300.0,
        lease_buffer: float = 30.0,
        poll_interval: float = 1.0,
        api: ApiConfig | bool | None = None,
    ) -> None:
        self.config = JobsConfig(
            database=database,
            default_timeout=default_timeout,
            lease_buffer=lease_buffer,
            poll_interval=poll_interval,
        )
        self.backend = _build_backend(self.config)
        self.manager = JobManager(self.backend, self.config)
        self.app = app
        set_current_manager(self.manager)

        if api:
            if app is None:
                raise ValueError("api was requested but no FastAPI app was passed to Jobs()")
            router_config = api if isinstance(api, ApiConfig) else ApiConfig()
            app.include_router(build_router(self.manager, router_config))
