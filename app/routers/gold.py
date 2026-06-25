from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database import GoldRecord, get_db, to_dict

router = APIRouter()


@router.get("/stocks")
def get_gold_stocks(
    ticker: str | None = None,
    limit: int = Query(default=20, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
):
    query = db.query(GoldRecord)
    if ticker:
        query = query.filter(GoldRecord.ticker == ticker.upper())

    total = query.count()
    page = query.order_by(GoldRecord.processed_at.desc()).offset(offset).limit(limit).all()
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": (offset + limit) < total,
        "records": [to_dict(r) for r in page],
    }
