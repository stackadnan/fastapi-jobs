from __future__ import annotations

from pathlib import Path

import pytest

from fastapi_jobs.backends.sqlite import SQLiteBackend
from fastapi_jobs.config import JobsConfig
from fastapi_jobs.decorators import _registry
from fastapi_jobs.manager import JobManager, set_current_manager


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "jobs.db"


@pytest.fixture
async def backend(db_path: Path) -> SQLiteBackend:
    backend = SQLiteBackend(f"sqlite:///{db_path}")
    await backend.initialize()
    return backend


@pytest.fixture
async def manager(backend: SQLiteBackend) -> JobManager:
    manager = JobManager(backend, JobsConfig())
    set_current_manager(manager)
    return manager


@pytest.fixture(autouse=True)
def clean_task_registry():
    # @task registration is process-global by design (see decorators.py), so tests
    # that define tasks at module scope would otherwise collide across test modules.
    before = set(_registry)
    yield
    for name in set(_registry) - before:
        del _registry[name]


@pytest.fixture(autouse=True)
def reset_current_manager():
    # The "current Jobs instance" binding is process-global too (see manager.py), so a
    # test that clears it (e.g. to exercise JobsNotConfiguredError) must not leak that
    # into whichever test runs next.
    yield
    set_current_manager(None)
