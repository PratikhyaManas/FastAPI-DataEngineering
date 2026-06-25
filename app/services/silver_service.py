from datetime import datetime, timezone
from typing import Any, Optional


def transform_bronze_to_silver_stock(bronze: dict[str, Any]) -> Optional[dict[str, Any]]:
    if not bronze.get("is_valid", False):
        return None

    open_price = bronze.get("open_price")
    close_price = bronze.get("close_price")
    volume = bronze.get("volume")
    trade_date = bronze.get("trade_date")
    ticker = bronze.get("ticker")

    if None in (open_price, close_price, volume, trade_date, ticker):
        return None

    daily_return_pct = None
    if open_price and open_price > 0:
        daily_return_pct = round(((close_price - open_price) / open_price) * 100, 4)

    if volume >= 1_000_000:
        volume_category = "HIGH"
    elif volume >= 200_000:
        volume_category = "MEDIUM"
    else:
        volume_category = "LOW"

    return {
        "record_id": bronze["record_id"],
        "source_record_id": bronze["record_id"],
        "ticker": ticker,
        "trade_date": trade_date,
        "open_price": open_price,
        "close_price": close_price,
        "volume": volume,
        "daily_return_pct": daily_return_pct,
        "vwap_estimate": round((open_price + close_price) / 2, 4),
        "is_bullish": close_price > open_price,
        "volume_category": volume_category,
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }
