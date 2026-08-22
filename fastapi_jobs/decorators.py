from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal, overload

from fastapi_jobs.manager import get_current_manager
from fastapi_jobs.models import Job, RetryPolicy

_registry: dict[str, Task] = {}


class Task:
    """The handle returned by @task. Callable directly for testing; .enqueue() queues it."""

    def __init__(
        self,
        func: Callable[..., Any],
        *,
        name: str,
        retry_policy: RetryPolicy,
        timeout: float | None,
    ) -> None:
        self.func = func
        self.name = name
        self.retry_policy = retry_policy
        self.timeout = timeout

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.func(*args, **kwargs)

    async def enqueue(self, *args: Any, delay: float = 0.0, **kwargs: Any) -> Job:
        manager = get_current_manager()
        return await manager.enqueue(self, args, kwargs, delay=delay)

    def __repr__(self) -> str:
        return f"Task({self.name!r})"


def get_task(name: str) -> Task | None:
    return _registry.get(name)


@overload
def task(func: Callable[..., Any]) -> Task: ...
@overload
def task(
    func: None = None,
    *,
    retries: int = 0,
    timeout: float | None = None,
    backoff: Literal["fixed", "exponential"] = "exponential",
    retry_on: tuple[type[BaseException], ...] = (Exception,),
    base_delay: float = 1.0,
    max_delay: float = 300.0,
    jitter: bool = True,
    name: str | None = None,
) -> Callable[[Callable[..., Any]], Task]: ...


def task(
    func: Callable[..., Any] | None = None,
    *,
    retries: int = 0,
    timeout: float | None = None,
    backoff: Literal["fixed", "exponential"] = "exponential",
    retry_on: tuple[type[BaseException], ...] = (Exception,),
    base_delay: float = 1.0,
    max_delay: float = 300.0,
    jitter: bool = True,
    name: str | None = None,
) -> Task | Callable[[Callable[..., Any]], Task]:
    if retries < 0:
        raise ValueError("retries must be >= 0")
    if timeout is not None and timeout <= 0:
        raise ValueError("timeout must be > 0")

    def decorator(fn: Callable[..., Any]) -> Task:
        task_name = name or f"{fn.__module__}.{fn.__qualname__}"
        if task_name in _registry:
            raise ValueError(f"a task named '{task_name}' is already registered")
        registered = Task(
            fn,
            name=task_name,
            retry_policy=RetryPolicy(
                max_attempts=retries + 1,
                backoff=backoff,
                base_delay=base_delay,
                max_delay=max_delay,
                jitter=jitter,
                retry_on=retry_on,
            ),
            timeout=timeout,
        )
        _registry[task_name] = registered
        return registered

    if func is not None:
        return decorator(func)
    return decorator
