from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.database import RawRecord, get_db
from app.models.pipeline_models import RawStockPrice

router = APIRouter()


@router.post("/stocks", status_code=201)
def ingest_stocks(records: list[RawStockPrice], db: Session = Depends(get_db)):
    added = 0
    duplicates = 0
    updated = 0

    incoming_keys = {(r.ticker.upper(), r.trade_date) for r in records}
    incoming_tickers = {k[0] for k in incoming_keys}
    incoming_dates = {k[1] for k in incoming_keys}
    if incoming_tickers and incoming_dates:
        existing_rows = db.execute(
            select(RawRecord).where(
                RawRecord.ticker.in_(incoming_tickers),
                RawRecord.trade_date.in_(incoming_dates),
            )
        ).scalars().all()
        existing_map = {(r.ticker, r.trade_date): r for r in existing_rows}
    else:
        existing_map = {}

    to_insert: list[RawRecord] = []

    for record in records:
        key = (record.ticker.upper(), record.trade_date)
        existing = existing_map.get(key)
        if existing is not None:
            # Late corrections should advance received_at to be picked by incremental watermarks.
            existing.open_price = record.open_price
            existing.close_price = record.close_price
            existing.volume = record.volume
            existing.received_at = datetime.now(timezone.utc)
            updated += 1
            duplicates += 1
            continue

        to_insert.append(
            RawRecord(
                ticker=key[0],
                open_price=record.open_price,
                close_price=record.close_price,
                volume=record.volume,
                trade_date=record.trade_date,
                received_at=datetime.now(timezone.utc),
            )
        )
        existing_map[key] = to_insert[-1]
        added += 1

    if to_insert:
        db.bulk_save_objects(to_insert)
    db.commit()

    return {
        "records_received": len(records),
        "records_added": added,
        "records_updated": updated,
        "duplicates_skipped": duplicates,
    }


@router.post("/sample/stocks", status_code=201)
def ingest_sample_stocks(
    days: int = Query(default=5, ge=1, le=30),
    inject_errors: bool = False,
    db: Session = Depends(get_db),
):
    sample = []
    tickers = ["AAPL", "MSFT", "AMZN", "TSLA", "NVDA"]
    base_date = datetime.now(timezone.utc).date()

    for i in range(days):
        for j, ticker in enumerate(tickers):
            trade_date = (base_date - timedelta(days=i)).isoformat()
            open_p = 100 + (j * 10) + i
            close_p = open_p + ((-1) ** j) * (i + 1)
            volume = 100000 * (j + 1) + (i * 1000)

            sample.append(
                {
                    "ticker": ticker,
                    "open_price": str(open_p),
                    "close_price": str(close_p),
                    "volume": str(volume),
                    "trade_date": trade_date,
                }
            )

    if inject_errors and sample:
        sample[0]["close_price"] = "bad_float"
        sample[1]["trade_date"] = "2026/01/01"
        sample[2]["ticker"] = ""

    return ingest_stocks([RawStockPrice(**r) for r in sample if r.get("ticker") != ""], db)
