from __future__ import annotations

import asyncio

import pytest

from fastapi_jobs.decorators import Task, task
from fastapi_jobs.exceptions import JobsNotConfiguredError
from fastapi_jobs.manager import set_current_manager


def test_task_registers_with_qualified_name():
    @task
    def greet(name: str) -> str:
        return f"hi {name}"

    assert isinstance(greet, Task)
    assert greet.name.endswith("test_task_registers_with_qualified_name.<locals>.greet")


def test_task_is_directly_callable_for_unit_testing():
    @task
    def add(a: int, b: int) -> int:
        return a + b

    assert add(2, 3) == 5


def test_retries_maps_to_max_attempts():
    @task(retries=3)
    def flaky() -> None:
        pass

    assert flaky.retry_policy.max_attempts == 4


def test_default_has_no_retries():
    @task
    def once() -> None:
        pass

    assert once.retry_policy.max_attempts == 1


def test_duplicate_task_name_raises():
    @task(name="dup")
    def first() -> None:
        pass

    with pytest.raises(ValueError, match="already registered"):

        @task(name="dup")
        def second() -> None:
            pass


def test_negative_retries_rejected():
    with pytest.raises(ValueError, match="retries must be"):

        @task(retries=-1)
        def bad() -> None:
            pass


def test_enqueue_without_jobs_instance_raises():
    set_current_manager(None)

    @task
    def orphan() -> None:
        pass

    with pytest.raises(JobsNotConfiguredError):
        asyncio.run(orphan.enqueue())
