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
    assert response.status_code == 401


def test_jwt_operator_can_trigger_pipeline():
    token_resp = client.post(
        "/auth/token",
        json={"username": "pipeline-user", "role": "operator"},
    )
    assert token_resp.status_code == 200
    token = token_resp.json()["access_token"]

    client.post("/ingest/sample/stocks?days=1")
    run = client.post(
        "/pipeline/run/bronze",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert run.status_code == 200


def test_jwt_reader_cannot_trigger_operator_endpoint():
    token_resp = client.post(
        "/auth/token",
        json={"username": "read-user", "role": "reader"},
    )
    assert token_resp.status_code == 200
    token = token_resp.json()["access_token"]

    client.post("/ingest/sample/stocks?days=1")
    run = client.post(
        "/pipeline/run/bronze",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert run.status_code == 403


def test_incremental_full_pipeline_first_run_processes_records():
    client.post("/ingest/sample/stocks?days=2")
    response = client.post("/pipeline/run/full/incremental", headers=AUTH_HEADERS)
    assert response.status_code == 200

    payload = response.json()
    assert payload["status"] == "success"
    assert payload["stages"][0]["records_out"] > 0


def test_incremental_full_pipeline_second_run_is_noop():
    client.post("/ingest/sample/stocks?days=2")
    first = client.post("/pipeline/run/full/incremental", headers=AUTH_HEADERS)
    assert first.status_code == 200

    second = client.post("/pipeline/run/full/incremental", headers=AUTH_HEADERS)
    assert second.status_code == 200

    payload = second.json()
    assert payload["stages"][0]["records_in"] == 0
    assert payload["stages"][1]["records_in"] == 0
    assert payload["stages"][2]["records_in"] == 0


def test_incremental_gold_upsert_updates_existing_aggregate():
    first_batch = [
        {
            "ticker": "AAPL",
            "open_price": "100",
            "close_price": "100",
            "volume": "100",
            "trade_date": "2026-01-01",
        }
    ]
    ingest_first = client.post("/ingest/stocks", json=first_batch)
    assert ingest_first.status_code == 201

    first_run = client.post("/pipeline/run/full/incremental", headers=AUTH_HEADERS)
    assert first_run.status_code == 200

    before = client.get("/gold/stocks")
    assert before.status_code == 200
    before_rows = [r for r in before.json()["records"] if r["ticker"] == "AAPL" and r["trade_date"] == "2026-01-01"]
    assert len(before_rows) == 1
    assert before_rows[0]["avg_close_price"] == 100.0
    assert before_rows[0]["total_volume"] == 100

    second_batch = [
        {
            "ticker": "AAPL",
            "open_price": "110",
            "close_price": "200",
            "volume": "300",
            "trade_date": "2026-01-01",
        }
    ]
    ingest_second = client.post("/ingest/stocks", json=second_batch)
    assert ingest_second.status_code == 201
    assert ingest_second.json()["records_updated"] == 1

    second_run = client.post("/pipeline/run/full/incremental", headers=AUTH_HEADERS)
    assert second_run.status_code == 200

    after = client.get("/gold/stocks")
    assert after.status_code == 200
    after_rows = [r for r in after.json()["records"] if r["ticker"] == "AAPL" and r["trade_date"] == "2026-01-01"]
    assert len(after_rows) == 1
    assert after_rows[0]["avg_close_price"] == 200.0
    assert after_rows[0]["total_volume"] == 300
