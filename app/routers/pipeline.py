import time
import uuid
import json
from datetime import datetime, timezone
from typing import Callable

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import (
    BronzeRecord,
    DQResult,
    GoldRecord,
    PipelineJob,
    PipelineState,
    RawRecord,
    SessionLocal,
    SilverRecord,
    get_db,
    set_job_result,
    to_dict,
)
from app.security import Role, require_role
from app.services import bronze_service, gold_service, silver_service

router = APIRouter()
_STAGE_NAMES = {"bronze", "silver", "gold", "full"}


def _parse_iso_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _get_state(db: Session, stage: str) -> PipelineState:
    state = db.get(PipelineState, stage)
    if state is None:
        state = PipelineState(stage=stage, records_processed=0, watermark=datetime.now(timezone.utc), status="idle")
        db.add(state)
        db.commit()
        db.refresh(state)
    return state


def _run_bronze_stage(db: Session) -> dict:
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
    bronze_models = [
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
    if bronze_models:
        db.bulk_save_objects(bronze_models)

    dq_checks = bronze_service.run_bronze_dq_checks(bronze, "bronze_stock_prices")
    db.query(DQResult).delete()
    dq_models = [
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
    if dq_models:
        db.bulk_save_objects(dq_models)

    valid = sum(1 for r in bronze if r.get("is_valid"))
    invalid = len(bronze) - valid
    now = datetime.now(timezone.utc)
    state.last_processed_at = now
    state.records_processed += valid
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


def _run_silver_stage(db: Session) -> dict:
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
    silver_models = [
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
    if silver_models:
        db.bulk_save_objects(silver_models)

    now = datetime.now(timezone.utc)
    state.last_processed_at = now
    state.records_processed += len(transformed)
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


def _run_gold_stage(db: Session) -> dict:
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
    gold_models = [
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
    if gold_models:
        db.bulk_save_objects(gold_models)

    now = datetime.now(timezone.utc)
    state.last_processed_at = now
    state.records_processed += len(gold_rows)
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


def _run_full_stage(db: Session) -> dict:
    bronze = _run_bronze_stage(db)
    silver = _run_silver_stage(db)
    gold = _run_gold_stage(db)
    return {"status": "success", "stages": [bronze, silver, gold]}


def _run_bronze_stage_incremental(db: Session) -> dict:
    state = _get_state(db, "raw_to_bronze")
    start = time.perf_counter()
    state.status = "running"
    db.commit()

    watermark = state.watermark
    raw_query = db.query(RawRecord)
    if watermark is not None:
        raw_query = raw_query.filter(RawRecord.received_at > watermark)
    raw_rows = raw_query.all()

    if not raw_rows:
        now = datetime.now(timezone.utc)
        state.last_processed_at = now
        state.status = "idle"
        db.commit()
        return {
            "stage": "raw_to_bronze_incremental",
            "status": "success",
            "records_in": 0,
            "records_out": 0,
            "records_valid": 0,
            "records_invalid": 0,
            "duration_ms": round((time.perf_counter() - start) * 1000, 2),
            "dq_checks": [],
            "message": "No new raw records since watermark.",
        }

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

    existing_keys = {
        (r.ticker, r.trade_date)
        for r in db.query(BronzeRecord.ticker, BronzeRecord.trade_date)
        .filter(BronzeRecord.ticker.isnot(None), BronzeRecord.trade_date.isnot(None))
        .all()
    }

    new_bronze_models: list[BronzeRecord] = []
    for r in bronze:
        td = datetime.fromisoformat(r["trade_date"]).date() if r["trade_date"] else None
        key = (r["ticker"], td)
        if key in existing_keys:
            continue
        existing_keys.add(key)
        new_bronze_models.append(
            BronzeRecord(
                record_id=r["record_id"],
                ticker=r["ticker"],
                open_price=r["open_price"],
                close_price=r["close_price"],
                volume=r["volume"],
                trade_date=td,
                ingested_at=datetime.fromisoformat(r["ingested_at"]),
                is_valid=bool(r["is_valid"]),
                error_reason=r["error_reason"],
            )
        )

    if new_bronze_models:
        db.bulk_save_objects(new_bronze_models)

    dq_checks = bronze_service.run_bronze_dq_checks(bronze, "bronze_stock_prices_incremental")
    if dq_checks:
        db.bulk_save_objects(
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
    max_received = max((row.received_at for row in raw_rows), default=now)
    state.last_processed_at = now
    state.records_processed += valid
    state.watermark = max_received
    state.status = "idle"
    db.commit()

    return {
        "stage": "raw_to_bronze_incremental",
        "status": "success",
        "records_in": len(raw_rows),
        "records_out": len(new_bronze_models),
        "records_valid": valid,
        "records_invalid": invalid,
        "duration_ms": round((time.perf_counter() - start) * 1000, 2),
        "dq_checks": dq_checks,
    }


def _run_silver_stage_incremental(db: Session) -> dict:
    state = _get_state(db, "bronze_to_silver")
    start = time.perf_counter()
    state.status = "running"
    db.commit()

    watermark = state.watermark
    bronze_query = db.query(BronzeRecord)
    if watermark is not None:
        bronze_query = bronze_query.filter(BronzeRecord.ingested_at > watermark)
    bronze_rows = bronze_query.all()

    if not bronze_rows:
        now = datetime.now(timezone.utc)
        state.last_processed_at = now
        state.status = "idle"
        db.commit()
        return {
            "stage": "bronze_to_silver_incremental",
            "status": "success",
            "records_in": 0,
            "records_out": 0,
            "duration_ms": round((time.perf_counter() - start) * 1000, 2),
            "message": "No new bronze records since watermark.",
        }

    existing_ids = {rid for (rid,) in db.query(SilverRecord.source_record_id).all()}
    transformed = []
    for rec in bronze_rows:
        if rec.record_id in existing_ids:
            continue
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

    silver_models = [
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
    if silver_models:
        db.bulk_save_objects(silver_models)

    now = datetime.now(timezone.utc)
    max_ingested = max((row.ingested_at for row in bronze_rows), default=now)
    state.last_processed_at = now
    state.records_processed += len(transformed)
    state.watermark = max_ingested
    state.status = "idle"
    db.commit()

    return {
        "stage": "bronze_to_silver_incremental",
        "status": "success",
        "records_in": len(bronze_rows),
        "records_out": len(transformed),
        "duration_ms": round((time.perf_counter() - start) * 1000, 2),
    }


def _run_gold_stage_incremental(db: Session) -> dict:
    state = _get_state(db, "silver_to_gold")
    start = time.perf_counter()
    state.status = "running"
    db.commit()

    watermark = state.watermark
    silver_query = db.query(SilverRecord)
    if watermark is not None:
        silver_query = silver_query.filter(SilverRecord.processed_at > watermark)
    silver_rows = silver_query.all()

    if not silver_rows:
        now = datetime.now(timezone.utc)
        state.last_processed_at = now
        state.status = "idle"
        db.commit()
        return {
            "stage": "silver_to_gold_incremental",
            "status": "success",
            "records_in": 0,
            "records_out": 0,
            "duration_ms": round((time.perf_counter() - start) * 1000, 2),
            "message": "No new silver records since watermark.",
        }

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

    existing_keys = {
        (r.ticker, r.trade_date)
        for r in db.query(GoldRecord.ticker, GoldRecord.trade_date)
        .all()
    }
    new_gold_models: list[GoldRecord] = []
    for r in gold_rows:
        trade_date = datetime.fromisoformat(r["trade_date"]).date()
        key = (r["ticker"], trade_date)
        if key in existing_keys:
            continue
        existing_keys.add(key)
        new_gold_models.append(
            GoldRecord(
                record_id=r["record_id"],
                ticker=r["ticker"],
                trade_date=trade_date,
                avg_close_price=float(r["avg_close_price"]),
                avg_daily_return_pct=r["avg_daily_return_pct"],
                total_volume=int(r["total_volume"]),
                bullish_days=int(r["bullish_days"]),
                bearish_days=int(r["bearish_days"]),
                source_count=int(r["source_count"]),
                processed_at=datetime.fromisoformat(r["processed_at"]),
            )
        )

    if new_gold_models:
        db.bulk_save_objects(new_gold_models)

    now = datetime.now(timezone.utc)
    max_processed = max((row.processed_at for row in silver_rows), default=now)
    state.last_processed_at = now
    state.records_processed += len(new_gold_models)
    state.watermark = max_processed
    state.status = "idle"
    db.commit()

    return {
        "stage": "silver_to_gold_incremental",
        "status": "success",
        "records_in": len(silver_rows),
        "records_out": len(new_gold_models),
        "duration_ms": round((time.perf_counter() - start) * 1000, 2),
    }


def _run_full_stage_incremental(db: Session) -> dict:
    bronze = _run_bronze_stage_incremental(db)
    silver = _run_silver_stage_incremental(db)
    gold = _run_gold_stage_incremental(db)
    return {"status": "success", "stages": [bronze, silver, gold]}


def _run_stage_by_name(stage: str, db: Session) -> dict:
    dispatch: dict[str, Callable[[Session], dict]] = {
        "bronze": _run_bronze_stage,
        "silver": _run_silver_stage,
        "gold": _run_gold_stage,
        "full": _run_full_stage,
    }
    runner = dispatch.get(stage)
    if runner is None:
        raise HTTPException(status_code=404, detail=f"Unknown stage: {stage}")
    return runner(db)


def _run_stage_by_name_incremental(stage: str, db: Session) -> dict:
    dispatch: dict[str, Callable[[Session], dict]] = {
        "bronze": _run_bronze_stage_incremental,
        "silver": _run_silver_stage_incremental,
        "gold": _run_gold_stage_incremental,
        "full": _run_full_stage_incremental,
    }
    runner = dispatch.get(stage)
    if runner is None:
        raise HTTPException(status_code=404, detail=f"Unknown stage: {stage}")
    return runner(db)


@router.post("/run/bronze")
def run_bronze_pipeline(
    db: Session = Depends(get_db),
    _: Role = Depends(require_role(Role.operator)),
):
    return _run_bronze_stage(db)


@router.post("/run/silver")
def run_silver_pipeline(
    db: Session = Depends(get_db),
    _: Role = Depends(require_role(Role.operator)),
):
    return _run_silver_stage(db)


@router.post("/run/gold")
def run_gold_pipeline(
    db: Session = Depends(get_db),
    _: Role = Depends(require_role(Role.operator)),
):
    return _run_gold_stage(db)


@router.post("/run/full")
def run_full_pipeline(
    db: Session = Depends(get_db),
    _: Role = Depends(require_role(Role.operator)),
):
    return _run_full_stage(db)


def _run_stage_job(job_id: str, stage: str) -> None:
    db = SessionLocal()
    try:
        job = db.get(PipelineJob, job_id)
        if job is None:
            return
        job.status = "running"
        job.started_at = datetime.now(timezone.utc)
        db.commit()

        try:
            if stage.endswith("_incremental"):
                result = _run_stage_by_name_incremental(stage.replace("_incremental", ""), db)
            else:
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
    if normalized not in _STAGE_NAMES:
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


@router.post("/run/bronze/incremental")
def run_bronze_incremental(
    db: Session = Depends(get_db),
    _: Role = Depends(require_role(Role.operator)),
):
    return _run_bronze_stage_incremental(db)


@router.post("/run/silver/incremental")
def run_silver_incremental(
    db: Session = Depends(get_db),
    _: Role = Depends(require_role(Role.operator)),
):
    return _run_silver_stage_incremental(db)


@router.post("/run/gold/incremental")
def run_gold_incremental(
    db: Session = Depends(get_db),
    _: Role = Depends(require_role(Role.operator)),
):
    return _run_gold_stage_incremental(db)


@router.post("/run/full/incremental")
def run_full_incremental(
    db: Session = Depends(get_db),
    _: Role = Depends(require_role(Role.operator)),
):
    return _run_full_stage_incremental(db)


@router.post("/run/{stage}/incremental/async", status_code=202)
def run_stage_incremental_async(
    stage: str,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    _: Role = Depends(require_role(Role.operator)),
):
    normalized = stage.strip().lower()
    if normalized not in _STAGE_NAMES:
        raise HTTPException(status_code=404, detail=f"Unknown stage: {stage}")

    job_id = str(uuid.uuid4())
    job = PipelineJob(
        job_id=job_id,
        stage=f"{normalized}_incremental",
        status="queued",
        created_at=datetime.now(timezone.utc),
        started_at=None,
        finished_at=None,
        message="Queued",
        result_json=None,
    )
    db.add(job)
    db.commit()

    background_tasks.add_task(_run_stage_job, job_id, f"{normalized}_incremental")
    return {
        "job_id": job_id,
        "status": "queued",
        "stage": f"{normalized}_incremental",
        "status_url": f"/pipeline/status/{job_id}",
    }
