from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import BronzeRecord, GoldRecord, SilverRecord, get_db, to_dict
from app.services.export_service import export_to_csv

router = APIRouter()


@router.post("/all")
def export_all_layers(partition_date: str | None = None, db: Session = Depends(get_db)):
    bronze_rows = db.query(BronzeRecord).all()
    silver_rows = db.query(SilverRecord).all()
    gold_rows = db.query(GoldRecord).all()

    if not (bronze_rows or silver_rows or gold_rows):
        raise HTTPException(status_code=400, detail="Nothing to export. Run pipeline first.")

    date_used = partition_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    outputs = {
        "bronze": export_to_csv([to_dict(x) for x in bronze_rows], "bronze", "stock_prices", date_used),
        "silver": export_to_csv([to_dict(x) for x in silver_rows], "silver", "stock_prices", date_used),
        "gold": export_to_csv([to_dict(x) for x in gold_rows], "gold", "ticker_summary", date_used),
    }
    return {"status": "success", "partition_date": date_used, "outputs": outputs}
