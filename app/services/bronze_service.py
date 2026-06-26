import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from app.config import settings
from app.services.dq_service import bronze_default_rules, evaluate_rules

_NULL_SENTINELS = {"none", "null", "n/a", "na", "nan", "", "-"}


def _safe_float(value: Any) -> tuple[Optional[float], Optional[str]]:
    if value is None:
        return None, None
    cleaned = str(value).strip().lower()
    if cleaned in _NULL_SENTINELS:
        return None, None
    try:
        return float(str(value).strip().replace(",", "")), None
    except (TypeError, ValueError):
        return None, f"Cannot cast '{value}' to float"


def _safe_int(value: Any) -> tuple[Optional[int], Optional[str]]:
    if value is None:
        return None, None
    cleaned = str(value).strip().lower()
    if cleaned in _NULL_SENTINELS:
        return None, None
    try:
        return int(float(str(value).strip().replace(",", ""))), None
    except (TypeError, ValueError):
        return None, f"Cannot cast '{value}' to int"


def _safe_date(value: Any) -> tuple[Optional[str], Optional[str]]:
    if value is None:
        return None, None
    text = str(value).strip()
    try:
        parsed = datetime.strptime(text, "%Y-%m-%d").date()
        return parsed.isoformat(), None
    except ValueError:
        return None, f"Cannot parse '{value}' as YYYY-MM-DD"


def transform_raw_to_bronze_stock(raw: dict[str, Any]) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    errors: list[str] = []

    open_price, err = _safe_float(raw.get("open_price"))
    if err:
        errors.append(err)

    close_price, err = _safe_float(raw.get("close_price"))
    if err:
        errors.append(err)

    volume, err = _safe_int(raw.get("volume"))
    if err:
        errors.append(err)

    trade_date, err = _safe_date(raw.get("trade_date"))
    if err:
        errors.append(err)

    ticker = str(raw.get("ticker", "")).strip().upper() or None
    if ticker is None:
        errors.append("Ticker is required")

    return {
        "record_id": str(uuid.uuid4()),
        "ticker": ticker,
        "open_price": open_price,
        "close_price": close_price,
        "volume": volume,
        "trade_date": trade_date,
        "ingested_at": now,
        "is_valid": len(errors) == 0,
        "error_reason": "; ".join(errors) if errors else None,
    }


def run_bronze_dq_checks(records: list[dict[str, Any]], table: str) -> list[dict[str, Any]]:
    rules = bronze_default_rules()
    predicates = {
        "validity_rate": lambda value: value >= settings.dq_bronze_validity_threshold,
        "null_close_price_rate": lambda value: value <= settings.dq_bronze_null_close_threshold,
    }
    results = evaluate_rules(records, table, rules, predicates)

    # Attach explicit threshold labels after config resolution for observability.
    for result in results:
        if result["check_name"] == "validity_rate":
            result["threshold"] = f">= {settings.dq_bronze_validity_threshold:.2f}"
        elif result["check_name"] == "null_close_price_rate":
            result["threshold"] = f"<= {settings.dq_bronze_null_close_threshold:.2f}"
    return results
