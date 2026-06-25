import time
import uuid
import json
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import (
    BronzeRecord,
    DQResult,
    GoldRecord,
    PipelineJob,
    PipelineState,
    RawRecord,
    SilverRecord,
    get_db,
    set_job_result,
    to_dict,
)
from app.security import Role, require_role
from app.services import bronze_service, gold_service, silver_service

router = APIRouter()


def _get_state(db: Session, stage: str) -> PipelineState:
    state = db.get(PipelineState, stage)
    if state is None:
        state = PipelineState(stage=stage, records_processed=0, watermark=datetime.now(timezone.utc), status="idle")
        db.add(state)
        db.commit()
        db.refresh(state)
    return state


@router.post("/run/bronze")
def run_bronze_pipeline(
    db: Session = Depends(get_db),
    _: Role = Depends(require_role(Role.operator)),
):
    raw_rows = db.query(RawRecord).all()
    if not raw_rows:
        raise HTTPException(status_code=400, detail="No raw records to process. Ingest first.")

    start = time.perf_counter()
    state = _get_state(db, "raw_to_bronze")
    state.status = "running"
    db.commit()

    bronze = [
        bronze_service.transform_raw_to_bronze_stock(
            {
                "ticker": row.ticker,
                "open_price": row.open_price,
                "close_price": row.close_price,
                "volume": row.volume,
                "trade_date": row.trade_date,
            }
        )
        for row in raw_rows
    ]
    db.query(BronzeRecord).delete()
    db.flush()
    db.add_all(
        [
            BronzeRecord(
                record_id=r["record_id"],
                ticker=r["ticker"],
                open_price=r["open_price"],
                close_price=r["close_price"],
                volume=r["volume"],
                trade_date=(datetime.fromisoformat(r["trade_date"]).date() if r["trade_date"] else None),
                ingested_at=datetime.fromisoformat(r["ingested_at"]),
                is_valid=bool(r["is_valid"]),
                error_reason=r["error_reason"],
            )
            for r in bronze
        ]
    )

    dq_checks = bronze_service.run_bronze_dq_checks(bronze, "bronze_stock_prices")
    db.query(DQResult).delete()
    db.flush()
    db.add_all(
        [
            DQResult(
                check_name=c["check_name"],
                layer=c["layer"],
                table_name=c["table"],
                passed=bool(c["passed"]),
                actual=float(c["actual"]),
                threshold=c["threshold"],
                checked_at=datetime.fromisoformat(c["checked_at"]),
            )
            for c in dq_checks
        ]
    )

    valid = sum(1 for r in bronze if r.get("is_valid"))
    invalid = len(bronze) - valid
    now = datetime.now(timezone.utc)
    state.last_processed_at = now
    state.records_processed = state.records_processed + valid
    state.watermark = now
    state.status = "idle"
    db.commit()

    return {
        "stage": "raw_to_bronze",
        "status": "success",
        "records_in": len(raw_rows),
        "records_out": len(bronze),
        "records_valid": valid,
        "records_invalid": invalid,
        "duration_ms": round((time.perf_counter() - start) * 1000, 2),
        "dq_checks": dq_checks,
    }


@router.post("/run/silver")
def run_silver_pipeline(
    db: Session = Depends(get_db),
    _: Role = Depends(require_role(Role.operator)),
):
    bronze_rows = db.query(BronzeRecord).all()
    if not bronze_rows:
        raise HTTPException(status_code=400, detail="No bronze records to process.")

    start = time.perf_counter()
    state = _get_state(db, "bronze_to_silver")
    state.status = "running"
    db.commit()

    transformed = []
    for rec in bronze_rows:
        out = silver_service.transform_bronze_to_silver_stock(
            {
                "record_id": rec.record_id,
                "ticker": rec.ticker,
                "open_price": rec.open_price,
                "close_price": rec.close_price,
                "volume": rec.volume,
                "trade_date": rec.trade_date.isoformat() if rec.trade_date else None,
                "is_valid": rec.is_valid,
            }
        )
        if out is not None:
            transformed.append(out)

    db.query(SilverRecord).delete()
    db.flush()
    db.add_all(
        [
            SilverRecord(
                record_id=r["record_id"],
                source_record_id=r["source_record_id"],
                ticker=r["ticker"],
                trade_date=datetime.fromisoformat(r["trade_date"]).date(),
                open_price=float(r["open_price"]),
                close_price=float(r["close_price"]),
                volume=int(r["volume"]),
                daily_return_pct=r["daily_return_pct"],
                vwap_estimate=r["vwap_estimate"],
                is_bullish=bool(r["is_bullish"]),
                volume_category=r["volume_category"],
                processed_at=datetime.fromisoformat(r["processed_at"]),
            )
            for r in transformed
        ]
    )

    now = datetime.now(timezone.utc)
    state.last_processed_at = now
    state.records_processed = state.records_processed + len(transformed)
    state.watermark = now
    state.status = "idle"
    db.commit()

    return {
        "stage": "bronze_to_silver",
        "status": "success",
        "records_in": len(bronze_rows),
        "records_out": len(transformed),
        "duration_ms": round((time.perf_counter() - start) * 1000, 2),
    }


@router.post("/run/gold")
def run_gold_pipeline(
    db: Session = Depends(get_db),
    _: Role = Depends(require_role(Role.operator)),
):
    silver_rows = db.query(SilverRecord).all()
    if not silver_rows:
        raise HTTPException(status_code=400, detail="No silver records to process.")

    start = time.perf_counter()
    state = _get_state(db, "silver_to_gold")
    state.status = "running"
    db.commit()

    silver_payload = [
        {
            "record_id": r.record_id,
            "source_record_id": r.source_record_id,
            "ticker": r.ticker,
            "trade_date": r.trade_date.isoformat(),
            "open_price": r.open_price,
            "close_price": r.close_price,
            "volume": r.volume,
            "daily_return_pct": r.daily_return_pct,
            "vwap_estimate": r.vwap_estimate,
            "is_bullish": r.is_bullish,
            "volume_category": r.volume_category,
            "processed_at": r.processed_at.isoformat(),
        }
        for r in silver_rows
    ]
    gold_rows = gold_service.aggregate_silver_to_gold(silver_payload)

    db.query(GoldRecord).delete()
    db.flush()
    db.add_all(
        [
            GoldRecord(
                record_id=r["record_id"],
                ticker=r["ticker"],
                trade_date=datetime.fromisoformat(r["trade_date"]).date(),
                avg_close_price=float(r["avg_close_price"]),
                avg_daily_return_pct=r["avg_daily_return_pct"],
                total_volume=int(r["total_volume"]),
                bullish_days=int(r["bullish_days"]),
                bearish_days=int(r["bearish_days"]),
                source_count=int(r["source_count"]),
                processed_at=datetime.fromisoformat(r["processed_at"]),
            )
            for r in gold_rows
        ]
    )

    now = datetime.now(timezone.utc)
    state.last_processed_at = now
    state.records_processed = state.records_processed + len(gold_rows)
    state.watermark = now
    state.status = "idle"
    db.commit()

    return {
        "stage": "silver_to_gold",
        "status": "success",
        "records_in": len(silver_rows),
        "records_out": len(gold_rows),
        "duration_ms": round((time.perf_counter() - start) * 1000, 2),
    }


@router.post("/run/full")
def run_full_pipeline(
    db: Session = Depends(get_db),
    _: Role = Depends(require_role(Role.operator)),
):
    bronze = run_bronze_pipeline(db, _)
    silver = run_silver_pipeline(db, _)
    gold = run_gold_pipeline(db, _)
    return {"status": "success", "stages": [bronze, silver, gold]}


def _run_stage_by_name(stage: str, db: Session) -> dict:
    if stage == "bronze":
        return run_bronze_pipeline(db, Role.operator)
    if stage == "silver":
        return run_silver_pipeline(db, Role.operator)
    if stage == "gold":
        return run_gold_pipeline(db, Role.operator)
    if stage == "full":
        return run_full_pipeline(db, Role.operator)
    raise HTTPException(status_code=404, detail=f"Unknown stage: {stage}")


def _run_stage_job(job_id: str, stage: str) -> None:
    from app.database import SessionLocal

    db = SessionLocal()
    try:
        job = db.get(PipelineJob, job_id)
        if job is None:
            return
        job.status = "running"
        job.started_at = datetime.now(timezone.utc)
        db.commit()

        try:
            result = _run_stage_by_name(stage, db)
            job.status = "success"
            job.finished_at = datetime.now(timezone.utc)
            job.message = "Completed"
            set_job_result(job, result)
            db.commit()
        except Exception as exc:
            job.status = "failed"
            job.finished_at = datetime.now(timezone.utc)
            job.message = str(exc)
            db.commit()
    finally:
        db.close()


@router.post("/run/{stage}/async", status_code=202)
def run_stage_async(
    stage: str,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    _: Role = Depends(require_role(Role.operator)),
):
    normalized = stage.strip().lower()
    if normalized not in {"bronze", "silver", "gold", "full"}:
        raise HTTPException(status_code=404, detail=f"Unknown stage: {stage}")

    job_id = str(uuid.uuid4())
    job = PipelineJob(
        job_id=job_id,
        stage=normalized,
        status="queued",
        created_at=datetime.now(timezone.utc),
        started_at=None,
        finished_at=None,
        message="Queued",
        result_json=None,
    )
    db.add(job)
    db.commit()

    background_tasks.add_task(_run_stage_job, job_id, normalized)
    return {
        "job_id": job_id,
        "status": "queued",
        "stage": normalized,
        "status_url": f"/pipeline/status/{job_id}",
    }


@router.get("/status/{job_id}")
def get_job_status(job_id: str, db: Session = Depends(get_db)):
    job = db.get(PipelineJob, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    return {
        "job_id": job.job_id,
        "stage": job.stage,
        "status": job.status,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "finished_at": job.finished_at.isoformat() if job.finished_at else None,
        "message": job.message,
        "result": json.loads(job.result_json) if job.result_json else None,
    }


@router.get("/jobs")
def list_jobs(limit: int = 20, db: Session = Depends(get_db)):
    rows = db.query(PipelineJob).order_by(PipelineJob.created_at.desc()).limit(max(1, min(limit, 200))).all()
    return {"total": len(rows), "records": [to_dict(r) for r in rows]}
