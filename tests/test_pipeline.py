import pytest
from fastapi.testclient import TestClient

from main import app

client = TestClient(app)
AUTH_HEADERS = {"X-API-Key": "change-me", "X-Role": "operator"}
ADMIN_HEADERS = {"X-API-Key": "change-me", "X-Role": "admin"}


@pytest.fixture(autouse=True)
def reset_state():
    client.delete("/health/reset", headers=ADMIN_HEADERS)
    yield
    client.delete("/health/reset", headers=ADMIN_HEADERS)


def test_health_endpoint():
    response = client.get("/health/")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "healthy"


def test_bronze_quarantine_invalid_records():
    client.post("/ingest/sample/stocks?inject_errors=true")
    run = client.post("/pipeline/run/bronze", headers=AUTH_HEADERS)
    assert run.status_code == 200

    quarantine = client.get("/bronze/quarantine")
    assert quarantine.status_code == 200
    assert quarantine.json()["total"] > 0


def test_full_pipeline_and_gold_query():
    client.post("/ingest/sample/stocks?days=3")
    run = client.post("/pipeline/run/full", headers=AUTH_HEADERS)
    assert run.status_code == 200

    gold = client.get("/gold/stocks")
    assert gold.status_code == 200
    assert gold.json()["total"] > 0


def test_export_after_full_pipeline():
    client.post("/ingest/sample/stocks?days=2")
    client.post("/pipeline/run/full", headers=AUTH_HEADERS)

    exported = client.post("/export/all")
    assert exported.status_code == 200
    payload = exported.json()
    assert payload["status"] == "success"
    assert "gold" in payload["outputs"]


def test_async_bronze_job_status():
    client.post("/ingest/sample/stocks?days=1")
    queued = client.post("/pipeline/run/bronze/async", headers=AUTH_HEADERS)
    assert queued.status_code == 202
    job_id = queued.json()["job_id"]

    status = client.get(f"/pipeline/status/{job_id}")
    assert status.status_code == 200
    assert status.json()["status"] in {"running", "success", "queued"}


def test_forbidden_without_api_key():
    client.post("/ingest/sample/stocks?days=1")
    response = client.post("/pipeline/run/bronze")
    assert response.status_code == 403
