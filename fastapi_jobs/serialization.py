from __future__ import annotations

import json
from typing import Any

from fastapi_jobs.exceptions import JobSerializationError


def encode_payload(task_name: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
    try:
        return json.dumps({"args": list(args), "kwargs": kwargs})
    except (TypeError, ValueError) as exc:
        raise JobSerializationError(
            f"arguments for task '{task_name}' are not JSON serializable: {exc}"
        ) from exc


def decode_payload(payload: str) -> tuple[tuple[Any, ...], dict[str, Any]]:
    data = json.loads(payload)
    return tuple(data["args"]), data["kwargs"]


def encode_result(task_name: str, result: Any) -> str | None:
    if result is None:
        return None
    try:
        return json.dumps(result)
    except (TypeError, ValueError) as exc:
        raise JobSerializationError(
            f"return value of task '{task_name}' is not JSON serializable: {exc}"
        ) from exc


def decode_result(raw: str | None) -> Any:
    return json.loads(raw) if raw is not None else None
