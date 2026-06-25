from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database import BronzeRecord, get_db, to_dict

router = APIRouter()


@router.get("/stocks")
def get_bronze_stocks(
    valid_only: bool = False,
    ticker: str | None = None,
    limit: int = Query(default=20, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
):
    query = db.query(BronzeRecord)
    if valid_only:
        query = query.filter(BronzeRecord.is_valid.is_(True))
    if ticker:
        query = query.filter(BronzeRecord.ticker == ticker.upper())

    total = query.count()
    page = query.order_by(BronzeRecord.ingested_at.desc()).offset(offset).limit(limit).all()
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": (offset + limit) < total,
        "records": [to_dict(r) for r in page],
    }


@router.get("/quarantine")
def get_quarantine_records(
    limit: int = Query(default=100, ge=1, le=2000),
    db: Session = Depends(get_db),
):
    query = db.query(BronzeRecord).filter(BronzeRecord.is_valid.is_(False))
    total = query.count()
    records = query.order_by(BronzeRecord.ingested_at.desc()).limit(limit).all()
    return {"total": total, "records": [to_dict(r) for r in records]}
