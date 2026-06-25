from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Query
from sqlalchemy import tuple_
from sqlalchemy.orm import Session

from app.database import RawRecord, get_db
from app.models.pipeline_models import RawStockPrice

router = APIRouter()


@router.post("/stocks", status_code=201)
def ingest_stocks(records: list[RawStockPrice], db: Session = Depends(get_db)):
    added = 0
    duplicates = 0

    incoming_keys = {(r.ticker.upper(), r.trade_date) for r in records}
    existing_rows = (
        db.query(RawRecord)
        .filter(tuple_(RawRecord.ticker, RawRecord.trade_date).in_(list(incoming_keys)))
        .all()
    )
    existing = {(r.ticker, r.trade_date) for r in existing_rows}

    for record in records:
        key = (record.ticker.upper(), record.trade_date)
        if key in existing:
            duplicates += 1
            continue

        db.add(
            RawRecord(
                ticker=key[0],
                open_price=record.open_price,
                close_price=record.close_price,
                volume=record.volume,
                trade_date=record.trade_date,
                received_at=datetime.now(timezone.utc),
            )
        )
        existing.add(key)
        added += 1

    db.commit()

    return {
        "records_received": len(records),
        "records_added": added,
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
