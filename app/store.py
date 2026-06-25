from typing import Any

raw_records: list[dict[str, Any]] = []
bronze_records: list[dict[str, Any]] = []
silver_records: list[dict[str, Any]] = []
gold_records: list[dict[str, Any]] = []

dq_results: list[dict[str, Any]] = []

pipeline_state: dict[str, dict[str, Any]] = {
    "raw_to_bronze": {
        "last_processed_at": None,
        "records_processed": 0,
        "watermark": "1900-01-01T00:00:00",
        "status": "idle",
    },
    "bronze_to_silver": {
        "last_processed_at": None,
        "records_processed": 0,
        "watermark": "1900-01-01T00:00:00",
        "status": "idle",
    },
    "silver_to_gold": {
        "last_processed_at": None,
        "records_processed": 0,
        "watermark": "1900-01-01T00:00:00",
        "status": "idle",
    },
}


def reset_store() -> None:
    raw_records.clear()
    bronze_records.clear()
    silver_records.clear()
    gold_records.clear()
    dq_results.clear()

    for stage in pipeline_state.values():
        stage["last_processed_at"] = None
        stage["records_processed"] = 0
        stage["watermark"] = "1900-01-01T00:00:00"
        stage["status"] = "idle"
