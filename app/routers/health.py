from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.database import (
    BronzeRecord,
    DQResult,
    GoldRecord,
    PipelineState,
    RawRecord,
    SilverRecord,
    get_db,
    reset_all_data,
    to_dict,
)
from app.security import Role, require_role

router = APIRouter()


@router.get("/")
def health_check(db: Session = Depends(get_db)):
    states = db.query(PipelineState).all()
    return {
        "status": "healthy",
        "time": datetime.now(timezone.utc).isoformat(),
        "pipeline": {
            "raw_records": db.query(RawRecord).count(),
            "bronze_records": db.query(BronzeRecord).count(),
            "silver_records": db.query(SilverRecord).count(),
            "gold_records": db.query(GoldRecord).count(),
        },
        "pipeline_state": {s.stage: to_dict(s) for s in states},
        "dq_checks_run": db.query(DQResult).count(),
    }


@router.get("/dq")
def dq_summary(db: Session = Depends(get_db)):
    rows = db.query(DQResult).all()
    total = len(rows)
    passed = sum(1 for x in rows if x.passed)
    return {
        "total_checks": total,
        "passed": passed,
        "failed": total - passed,
        "pass_rate": round((passed / total), 4) if total else None,
        "results": [to_dict(r) for r in rows],
    }


@router.delete("/reset")
def reset_pipeline_state(
    db: Session = Depends(get_db),
    _: Role = Depends(require_role(Role.admin)),
):
    reset_all_data(db)
    return {"status": "reset", "message": "Pipeline state cleared."}
