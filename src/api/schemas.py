"""Pydantic response models (blueprint §2).  Times are UTC ISO-8601 ``...Z`` strings."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class _Model(BaseModel):
    # forward-compatible: services may add keys without breaking the contract
    model_config = ConfigDict(extra="allow")


class Page(_Model):
    items: list[dict[str, Any]]
    total: int


# --- health / status -------------------------------------------------------------------------


class Health(_Model):
    status: str
    time: str


class LiveTable(_Model):
    rows: int
    latest_open_time: str | None = None


class LiveStatus(_Model):
    ohlcv: LiveTable
    features: LiveTable
    predictions: LiveTable


class ModelRef(_Model):
    model_id: str | None = None


class ModelsStatus(_Model):
    long: ModelRef
    short: ModelRef


class WorkerStatus(_Model):
    alive: bool
    last_heartbeat: str | None = None


class SyncStatus(_Model):
    running: bool
    job_id: str | None = None
    last_finished_at: str | None = None
    last_status: str | None = None
    auto_sync: bool


class DbStatus(_Model):
    state: str


class Status(_Model):
    asset_id: str | None
    symbol: str | None
    interval: str | None
    live: LiveStatus | None
    models: ModelsStatus
    worker: WorkerStatus
    sync: SyncStatus
    db: DbStatus


# --- market ----------------------------------------------------------------------------------


class MarketConfig(_Model):
    asset_id: str | None
    symbol: str | None
    interval: str | None
    windows: list[str]
    default_window: str


class Candle(_Model):
    t: str | None
    o: float | None
    h: float | None
    l: float | None  # noqa: E741
    c: float | None
    v: float | None = None


class CandlesResponse(_Model):
    tf: str
    from_: str | None = Field(default=None, alias="from")
    to: str | None = None
    candles: list[Candle]

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class Bar(_Model):
    t: str | None
    o: float | None
    h: float | None
    l: float | None  # noqa: E741
    c: float | None
    long_pct: float | None = None
    short_pct: float | None = None
    long_raw: float | None = None
    short_raw: float | None = None


class Thresholds(_Model):
    long_entry: float | None = None
    short_entry: float | None = None


class ActiveTrade(_Model):
    side: str | None = None
    entry_price: float | None = None
    stop_loss: float | None = None
    take_profit: float | None = None


class ChartResponse(_Model):
    window: str
    focus_hours: int
    from_: str | None = Field(default=None, alias="from")
    to: str | None = None
    bars: list[Bar]
    thresholds: Thresholds
    active_trade: ActiveTrade | None = None

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class TableHealth(_Model):
    table: str
    table_name: str
    exists: bool
    rows: int
    latest_time: str | None = None


# --- predictions -----------------------------------------------------------------------------


class PredictionPoint(_Model):
    t: str | None
    close: float | None = None
    long_pct: float | None = None
    short_pct: float | None = None
    long_raw: float | None = None
    short_raw: float | None = None


class PredictionHistory(_Model):
    items: list[PredictionPoint]


class LatestPrediction(_Model):
    t: str | None
    close: float | None = None
    long_pct: float | None = None
    short_pct: float | None = None
    long_model_id: str | None = None
    short_model_id: str | None = None


class Signal(_Model):
    t: str | None
    side: str
    pct: float | None = None
    price: float | None = None


class SignalList(_Model):
    items: list[Signal]


# --- trades ----------------------------------------------------------------------------------


class ClosedTrade(_Model):
    entry_time: str | None = None
    exit_time: str | None = None
    side: str | None = None
    entry_price: float | None = None
    exit_price: float | None = None
    fact_log_return: float | None = None
    tp_lr: float | None = None
    sl_lr: float | None = None
    exit_reason: str | None = None
    tp_spec: str | None = None
    sl_spec: str | None = None
    session_id: str | None = None


class ClosedTrades(_Model):
    items: list[ClosedTrade]
    total: int


class ActivePosition(_Model):
    side: str | None = None
    entry_time: str | None = None
    entry_price: float | None = None
    quantity: float | None = None
    stop_loss: float | None = None
    take_profit: float | None = None
    source: str


class BinanceFill(_Model):
    t: str | None
    side: str
    price: float | None = None
    qty: float | None = None
    realized_pnl: float | None = None


class BinanceFills(_Model):
    items: list[BinanceFill]
    source: str


# --- equity / strat --------------------------------------------------------------------------


class EquityPoint(_Model):
    t: str | None
    cum_fact_log_return: float | None = None
    direction: str | None = None


class EquityResponse(_Model):
    session_ids: dict[str, str | None]
    points: list[EquityPoint]
    series: dict[str, list[dict[str, Any]]] | None = None
    summary: dict[str, Any]


class StrategyMetrics(_Model):
    n_trades: int | None = None
    win_rate: float | None = None
    total_fact_log_return: float | None = None
    compounded_return_pct: float | None = None
    avg_hold_minutes: float | None = None


class StrategyCard(_Model):
    session_id: str | None = None
    fit_period: dict[str, Any] | None = None
    entry_cutoff: float | None = None
    tp_spec: str | None = None
    sl_spec: str | None = None
    max_hold_minutes: int | None = None
    metrics: StrategyMetrics


class ActiveStrategies(_Model):
    long: StrategyCard | None = None
    short: StrategyCard | None = None


class StrategySession(_Model):
    session_id: str
    model_id_long: str | None = None
    model_id_short: str | None = None
    status: str | None = None
    created_at: str | None = None


# --- registry --------------------------------------------------------------------------------


class RegistryActive(_Model):
    live_models: dict[str, str | None]
    strategy_sessions: dict[str, str | None]
    pending_deployments: list[dict[str, Any]]


class ModelDetail(_Model):
    registry: dict[str, Any] | None
    manifest: dict[str, Any]
    params: dict[str, Any]
    search_best: dict[str, Any]
    feature_count: int
    artifacts: list[dict[str, Any]]
    reports: list[dict[str, Any]]


# --- logs ------------------------------------------------------------------------------------


class LogLine(_Model):
    ts: str | None = None
    level: str | None = None
    logger: str | None = None
    msg: str


class LogResponse(_Model):
    lines: list[LogLine]
