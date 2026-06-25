from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import Boolean, Date, DateTime, Float, Integer, String, Text, UniqueConstraint, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from app.config import settings


class Base(DeclarativeBase):
    pass


class RawRecord(Base):
    __tablename__ = "raw_records"
    __table_args__ = (UniqueConstraint("ticker", "trade_date", name="uq_raw_ticker_date"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(String(10), index=True)
    open_price: Mapped[str] = mapped_column(String(50))
    close_price: Mapped[str] = mapped_column(String(50))
    volume: Mapped[str] = mapped_column(String(50))
    trade_date: Mapped[str] = mapped_column(String(20), index=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class BronzeRecord(Base):
    __tablename__ = "bronze_records"

    record_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    ticker: Mapped[str | None] = mapped_column(String(10), index=True, nullable=True)
    open_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    close_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    volume: Mapped[int | None] = mapped_column(Integer, nullable=True)
    trade_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    is_valid: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    error_reason: Mapped[str | None] = mapped_column(Text, nullable=True)


class SilverRecord(Base):
    __tablename__ = "silver_records"

    record_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    source_record_id: Mapped[str] = mapped_column(String(64), index=True)
    ticker: Mapped[str] = mapped_column(String(10), index=True)
    trade_date: Mapped[date] = mapped_column(Date, index=True)
    open_price: Mapped[float] = mapped_column(Float)
    close_price: Mapped[float] = mapped_column(Float)
    volume: Mapped[int] = mapped_column(Integer)
    daily_return_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    vwap_estimate: Mapped[float | None] = mapped_column(Float, nullable=True)
    is_bullish: Mapped[bool] = mapped_column(Boolean, default=False)
    volume_category: Mapped[str] = mapped_column(String(20))
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class GoldRecord(Base):
    __tablename__ = "gold_records"

    record_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    ticker: Mapped[str] = mapped_column(String(10), index=True)
    trade_date: Mapped[date] = mapped_column(Date, index=True)
    avg_close_price: Mapped[float] = mapped_column(Float)
    avg_daily_return_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    total_volume: Mapped[int] = mapped_column(Integer)
    bullish_days: Mapped[int] = mapped_column(Integer)
    bearish_days: Mapped[int] = mapped_column(Integer)
    source_count: Mapped[int] = mapped_column(Integer)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class DQResult(Base):
    __tablename__ = "dq_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    check_name: Mapped[str] = mapped_column(String(100), index=True)
    layer: Mapped[str] = mapped_column(String(40), index=True)
    table_name: Mapped[str] = mapped_column(String(100), index=True)
    passed: Mapped[bool] = mapped_column(Boolean, index=True)
    actual: Mapped[float] = mapped_column(Float)
    threshold: Mapped[str] = mapped_column(String(40))
    checked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class PipelineState(Base):
    __tablename__ = "pipeline_state"

    stage: Mapped[str] = mapped_column(String(40), primary_key=True)
    last_processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    records_processed: Mapped[int] = mapped_column(Integer, default=0)
    watermark: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(20), default="idle")


class PipelineJob(Base):
    __tablename__ = "pipeline_jobs"

    job_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    stage: Mapped[str] = mapped_column(String(40), index=True)
    status: Mapped[str] = mapped_column(String(20), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    result_json: Mapped[str | None] = mapped_column(Text, nullable=True)


database_url = settings.database_url
connect_args: dict[str, Any] = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
engine = create_engine(database_url, future=True, pool_pre_ping=True, connect_args=connect_args)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def init_db() -> None:
    Base.metadata.create_all(bind=engine)


def init_pipeline_state(db: Session) -> None:
    default_stages = ("raw_to_bronze", "bronze_to_silver", "silver_to_gold")
    now = datetime.now(timezone.utc)
    for stage in default_stages:
        row = db.get(PipelineState, stage)
        if row is None:
            db.add(
                PipelineState(
                    stage=stage,
                    last_processed_at=None,
                    records_processed=0,
                    watermark=now,
                    status="idle",
                )
            )
    db.commit()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def reset_all_data(db: Session) -> None:
    db.query(PipelineJob).delete()
    db.query(DQResult).delete()
    db.query(GoldRecord).delete()
    db.query(SilverRecord).delete()
    db.query(BronzeRecord).delete()
    db.query(RawRecord).delete()
    db.query(PipelineState).delete()
    db.commit()
    init_pipeline_state(db)


def to_dict(model: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for column in model.__table__.columns:
        value = getattr(model, column.name)
        if isinstance(value, datetime):
            payload[column.name] = value.isoformat()
        elif isinstance(value, date):
            payload[column.name] = value.isoformat()
        else:
            payload[column.name] = value
    return payload


def set_job_result(job: PipelineJob, result: dict[str, Any]) -> None:
    job.result_json = json.dumps(result, default=str)