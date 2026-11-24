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


class AssetInfo(_Model):
    asset_id: str
    symbol: str | None
    interval: str | None
    market: str | None
    is_default: bool
    has_data: bool
    active: bool


class AssetList(_Model):
    default_asset_id: str
    items: list[AssetInfo]


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


# --- Impulse Scout (epic_074 / t942) ---------------------------------------------------------


class ScoutRuleResult(_Model):
    """Trade plan + outcome of one exit rule (outcome fields null while the event is open)."""

    tp: float | None = None
    sl: float | None = None
    outcome: str | None = None          # tp | sl | timeout | invalid
    outcome_at: str | None = None
    exit_price: float | None = None
    net_bps: float | None = None


class ScoutEvent(_Model):
    """One ``scout_signals`` row; prediction fields are immutable (forward-log)."""

    event_id: str
    asset_id: str
    model_id: str
    cell_id: str | None = None
    t0: str                             # local low (event anchor)
    t_c: str                            # confirmation = prediction time
    label_end: str | None = None        # t_c + H: the outcome is known after this
    low_t0: float | None = None
    close_tc: float | None = None
    a_log: float | None = None
    sigma_w: float | None = None
    a_sigma: float | None = None
    p_star: float | None = None
    stop: float | None = None
    velocity_1h: float | None = None
    pretrend_3d: float | None = None
    log_sigma_1m: float | None = None
    drop_depth: float | None = None
    volume_ratio: float | None = None
    scorable: bool | None = None
    p_impulse: float | None = None
    p_false_start: float | None = None
    p_sideways: float | None = None
    oof_percentile: float | None = None
    top20: bool | None = None
    top10: bool | None = None
    entry: float | None = None
    rules: dict[str, ScoutRuleResult] = Field(default_factory=dict)
    status: str                         # open | resolved | invalid
    label_class: str | None = None      # impulse | false_start | sideways
    as_of: str | None = None
    scored_at: str | None = None        # write time
    backfilled: bool | None = None
    write_lag_s: int | None = None      # scored_at - t_c


class ScoutSignals(_Model):
    asset_id: str
    available: bool                     # False: no scout_signals table for the asset (e.g. bchusdt)
    items: list[ScoutEvent]


class ScoutCounts(_Model):
    total: int
    open: int
    resolved: int
    invalid: int
    top20: int
    top10: int
    backfilled: int
    as_of: str | None = None
    last_scored_at: str | None = None
    first_live_sync_at: str | None = None


class ScoutRuleMeta(_Model):
    key: str | None = None
    name: str | None = None
    description: str | None = None


class ScoutMeta(_Model):
    model_id: str
    display_name: str | None = None
    status: str
    experimental: bool
    scout_asset: bool
    thresholds: dict[str, float | None]
    live_start: str | None = None
    detect_anchor: str | None = None
    holdout2_start: str | None = None
    cell_id: str | None = None
    h_min: int | None = None
    train_assets: list[str] = Field(default_factory=list)
    oof_auc: float | None = None
    rules: list[ScoutRuleMeta] = Field(default_factory=list)
    freeze_hash: str | None = None


class ScoutLatest(_Model):
    asset_id: str
    available: bool
    meta: ScoutMeta
    counts: ScoutCounts | None = None
    open_events: list[ScoutEvent]
    recent: list[ScoutEvent]            # newest first
