from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.testclient import TestClient

from fastapi_jobs.app import Jobs
from fastapi_jobs.decorators import task
from fastapi_jobs.router import ApiConfig


def _jobs_app(tmp_path: Path, **kwargs) -> tuple[FastAPI, Jobs]:
    app = FastAPI()
    jobs = Jobs(app, database=f"sqlite:///{tmp_path / 'jobs.db'}", **kwargs)
    return app, jobs


async def test_api_is_not_mounted_by_default(tmp_path: Path):
    app, _ = _jobs_app(tmp_path)
    client = TestClient(app)
    assert client.get("/jobs").status_code == 404


async def test_api_true_mounts_default_routes(tmp_path: Path):
    app, jobs = _jobs_app(tmp_path, api=True)
    client = TestClient(app)

    @task
    async def greet(name: str) -> str:
        return f"hi {name}"

    job = await greet.enqueue("ada")

    resp = client.get(f"/jobs/{job.id}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["id"] == job.id
    assert body["task_name"] == greet.name
    assert body["status"] == "PENDING"
    assert body["args"] == ["ada"]


async def test_get_missing_job_returns_404(tmp_path: Path):
    app, _ = _jobs_app(tmp_path, api=True)
    client = TestClient(app)
    resp = client.get("/jobs/does-not-exist")
    assert resp.status_code == 404


async def test_list_jobs_filters_by_status(tmp_path: Path):
    app, jobs = _jobs_app(tmp_path, api=True)
    client = TestClient(app)

    @task
    async def noop() -> None:
        pass

    a = await noop.enqueue()
    await jobs.manager.backend.claim_job("worker-1", lease_buffer_seconds=30)

    resp = client.get("/jobs", params={"status": "PENDING"})
    assert resp.status_code == 200
    ids = {row["id"] for row in resp.json()}
    assert a.id not in ids  # a was claimed and is now RUNNING


async def test_cancel_pending_job_via_api(tmp_path: Path):
    app, jobs = _jobs_app(tmp_path, api=True)
    client = TestClient(app)

    @task
    async def noop() -> None:
        pass

    job = await noop.enqueue()
    resp = client.post(f"/jobs/{job.id}/cancel")
    assert resp.status_code == 200
    assert resp.json()["status"] == "CANCELLED"


async def test_cancel_running_job_via_api_returns_409(tmp_path: Path):
    app, jobs = _jobs_app(tmp_path, api=True)
    client = TestClient(app)

    @task
    async def noop() -> None:
        pass

    job = await noop.enqueue()
    await jobs.manager.backend.claim_job("worker-1", lease_buffer_seconds=30)

    resp = client.post(f"/jobs/{job.id}/cancel")
    assert resp.status_code == 409


async def test_api_requires_an_app(tmp_path: Path):
    with pytest.raises(ValueError, match="no FastAPI app"):
        Jobs(database=f"sqlite:///{tmp_path / 'jobs.db'}", api=True)


async def test_custom_prefix_and_pagination(tmp_path: Path):
    app, jobs = _jobs_app(
        tmp_path, api=ApiConfig(prefix="/admin/jobs", default_limit=1, max_limit=1)
    )
    client = TestClient(app)

    @task
    async def noop() -> None:
        pass

    await noop.enqueue()
    await noop.enqueue()

    resp = client.get("/admin/jobs")
    assert resp.status_code == 200
    assert len(resp.json()) == 1  # default_limit caps it even without ?limit=


async def test_dependencies_gate_access(tmp_path: Path):
    def require_token(x_token: str = Header(default="")) -> None:
        if x_token != "secret":
            raise HTTPException(status_code=401, detail="bad token")

    app, jobs = _jobs_app(tmp_path, api=ApiConfig(dependencies=[Depends(require_token)]))
    client = TestClient(app)

    assert client.get("/jobs").status_code == 401
    assert client.get("/jobs", headers={"x-token": "secret"}).status_code == 200
