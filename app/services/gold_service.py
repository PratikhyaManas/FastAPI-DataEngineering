import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any


def aggregate_silver_to_gold(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for rec in records:
        grouped[(rec["ticker"], rec["trade_date"])].append(rec)

    output: list[dict[str, Any]] = []
    for (ticker, trade_date), rows in grouped.items():
        count = len(rows)
        total_volume = sum(int(r["volume"]) for r in rows)
        avg_close = round(sum(float(r["close_price"]) for r in rows) / count, 4)

        return_values = [
            float(r["daily_return_pct"]) for r in rows if r.get("daily_return_pct") is not None
        ]
        avg_return = round(sum(return_values) / len(return_values), 4) if return_values else None

        bullish_days = sum(1 for r in rows if r.get("is_bullish"))
        output.append(
            {
                "record_id": str(uuid.uuid4()),
                "ticker": ticker,
                "trade_date": trade_date,
                "avg_close_price": avg_close,
                "avg_daily_return_pct": avg_return,
                "total_volume": total_volume,
                "bullish_days": bullish_days,
                "bearish_days": count - bullish_days,
                "source_count": count,
                "processed_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    return output
