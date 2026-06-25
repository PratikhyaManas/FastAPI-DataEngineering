from datetime import date
from typing import Optional

from pydantic import BaseModel, Field


class RawStockPrice(BaseModel):
    ticker: str = Field(min_length=1, max_length=10)
    open_price: str
    close_price: str
    volume: str
    trade_date: str


class BronzeStockPrice(BaseModel):
    record_id: str
    ticker: Optional[str] = None
    open_price: Optional[float] = None
    close_price: Optional[float] = None
    volume: Optional[int] = None
    trade_date: Optional[date] = None
    ingested_at: str
    is_valid: bool
    error_reason: Optional[str] = None


class SilverStockPrice(BaseModel):
    record_id: str
    source_record_id: str
    ticker: str
    trade_date: date
    open_price: float
    close_price: float
    volume: int
    daily_return_pct: Optional[float] = None
    vwap_estimate: Optional[float] = None
    is_bullish: bool
    volume_category: str
    processed_at: str


class GoldTickerSummary(BaseModel):
    record_id: str
    ticker: str
    trade_date: date
    avg_close_price: float
    avg_daily_return_pct: Optional[float] = None
    total_volume: int
    bullish_days: int
    bearish_days: int
    source_count: int
    processed_at: str


class PipelineRunResult(BaseModel):
    stage: str
    status: str
    records_in: int
    records_out: int
    records_invalid: int = 0
    duration_ms: float
