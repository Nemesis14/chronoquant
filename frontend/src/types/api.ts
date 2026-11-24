// Response types for the BFF REST contract (_doc_/_plans_/068_bff_ui.md §2).
// Times are UTC ISO-8601 strings ("...Z").

export interface LiveTable { rows: number; latest_open_time: string | null }
export interface ModelRef { model_id: string | null }

export interface Status {
  asset_id: string | null
  symbol: string | null
  interval: string | null
  live: { ohlcv: LiveTable; features: LiveTable; predictions: LiveTable } | null
  models: { long: ModelRef; short: ModelRef }
  worker: { alive: boolean; last_heartbeat: string | null }
  sync: {
    running: boolean
    job_id: string | null
    last_finished_at: string | null
    last_status: string | null
    auto_sync: boolean
  }
  db: { state: 'ok' | 'busy' | string }
}

export interface Bar {
  t: string
  o: number | null
  h: number | null
  l: number | null
  c: number | null
  long_pct: number | null
  short_pct: number | null
  long_raw: number | null
  short_raw: number | null
}

export interface ActiveTradeOverlay {
  side: string | null
  entry_price: number | null
  stop_loss: number | null
  take_profit: number | null
}

export interface ChartResponse {
  window: string
  focus_hours: number
  from: string | null
  to: string | null
  bars: Bar[]
  thresholds: { long_entry: number | null; short_entry: number | null }
  active_trade: ActiveTradeOverlay | null
}

export interface ActivePosition {
  side: string | null
  entry_time: string | null
  entry_price: number | null
  quantity: number | null
  stop_loss: number | null
  take_profit: number | null
  source: 'journal' | 'binance' | string
}

export interface ClosedTrade {
  entry_time: string | null
  exit_time: string | null
  side: string | null
  entry_price: number | null
  exit_price: number | null
  fact_log_return: number | null
  tp_lr: number | null
  sl_lr: number | null
  exit_reason: string | null
  tp_spec: string | null
  sl_spec: string | null
  session_id: string | null
}

export interface Page<T> { items: T[]; total: number }

export interface BinanceFill {
  t: string | null
  side: string
  price: number | null
  qty: number | null
  realized_pnl: number | null
}

export type Row = Record<string, string | number | boolean | null>

export interface EquityPoint { t: string; cum_fact_log_return: number | null }
export interface EquityResponse {
  session_ids: Record<string, string | null>
  points: (EquityPoint & { direction?: string })[]
  series: Record<string, EquityPoint[]> | null
  summary: Record<string, StrategyMetrics>
}

export interface StrategyMetrics {
  n_trades: number | null
  win_rate: number | null
  total_fact_log_return: number | null
  compounded_return_pct: number | null
  avg_hold_minutes: number | null
}

export interface StrategyCard {
  session_id: string | null
  fit_period: { start?: string; end?: string } | null
  entry_cutoff: number | null
  tp_spec: string | null
  sl_spec: string | null
  max_hold_minutes: number | null
  metrics: StrategyMetrics
}
export interface ActiveStrategies { long: StrategyCard | null; short: StrategyCard | null }

export interface RegistryActive {
  live_models: Record<string, string | null>
  strategy_sessions: Record<string, string | null>
  pending_deployments: Record<string, unknown>[]
}

export interface LogLine { ts: string | null; level: string | null; logger: string | null; msg: string }

export type JobStatus = 'queued' | 'running' | 'succeeded' | 'failed' | 'canceled'
export interface JobResult {
  ohlcv_rows_before?: number
  ohlcv_rows_after?: number
  inserted_ohlcv_rows?: number
  start_time?: string
  end_time?: string
  [k: string]: unknown
}
export interface Job {
  id: string
  kind: string
  status: JobStatus
  created_at: string | null
  started_at: string | null
  finished_at: string | null
  params: Record<string, unknown>
  result: JobResult | null
  error: string | null
  log_size?: number
}
export interface WorkerInfo {
  alive: boolean
  pid: number | null
  started_at: string | null
  last_heartbeat: string | null
  current_job_id: string | null
}
export interface AutoSync { enabled: boolean; delay_seconds: number; next_due_in_s: number | null }

export interface AssetItem {
  asset_id: string
  symbol: string
  interval: string
  market: string
  is_default: boolean
  has_data: boolean
  active: boolean
}
export interface AssetList { default_asset_id: string; items: AssetItem[] }

/** /api/market/candles: OHLCV resampled to tf over [end - hours, end] (end defaults/caps at the latest bar). */
export interface Candle { t: string; o: number | null; h: number | null; l: number | null; c: number | null; v?: number | null }
export interface CandlesResponse { tf: string; from: string | null; to: string | null; candles: Candle[] }

// --- Impulse Scout (epic_074 / t942): experimental, NO-GO research model -- not a trading signal ---
export type ScoutStatus = 'open' | 'resolved' | 'invalid'
export type ScoutOutcome = 'tp' | 'sl' | 'timeout' | 'invalid'
export type ScoutLabel = 'impulse' | 'false_start' | 'sideways'

/** Trade plan + outcome of one exit rule (outcome fields null while the event is open). */
export interface ScoutRuleResult {
  tp: number | null
  sl: number | null
  outcome: ScoutOutcome | null
  outcome_at: string | null
  exit_price: number | null
  net_bps: number | null
}

/** One scout_signals row (times UTC ISO-Z). Prediction fields are immutable (forward-log). */
export interface ScoutEvent {
  event_id: string
  asset_id: string
  model_id: string
  cell_id: string | null
  t0: string // local low (event anchor)
  t_c: string // confirmation = prediction time
  label_end: string | null // t_c + H: outcome known after this
  low_t0: number | null
  close_tc: number | null
  a_log: number | null
  sigma_w: number | null
  a_sigma: number | null
  p_star: number | null
  stop: number | null
  velocity_1h: number | null
  pretrend_3d: number | null
  log_sigma_1m: number | null
  drop_depth: number | null
  volume_ratio: number | null
  scorable: boolean | null
  p_impulse: number | null
  p_false_start: number | null
  p_sideways: number | null
  oof_percentile: number | null
  top20: boolean | null
  top10: boolean | null
  entry: number | null
  rules: Record<string, ScoutRuleResult> // keys: 'default', 'r2_03'
  status: ScoutStatus
  label_class: ScoutLabel | null
  as_of: string | null
  scored_at: string | null // write time
  backfilled: boolean | null
  write_lag_s: number | null // scored_at - t_c
}

export interface ScoutSignals {
  asset_id: string
  available: boolean // false: no scout_signals table for the asset (e.g. bchusdt)
  items: ScoutEvent[] // ascending by t0
}

export interface ScoutCounts {
  total: number
  open: number
  resolved: number
  invalid: number
  top20: number
  top10: number
  backfilled: number
  as_of: string | null
  last_scored_at: string | null
  first_live_sync_at: string | null
}

export interface ScoutMeta {
  model_id: string
  display_name: string | null
  status: string // 'experimental_no_go'
  experimental: boolean
  scout_asset: boolean
  thresholds: { top20: number | null; top10: number | null }
  live_start: string | null
  detect_anchor: string | null
  holdout2_start: string | null
  cell_id: string | null
  h_min: number | null
  train_assets: string[]
  oof_auc: number | null
  rules: { key: string | null; name: string | null; description: string | null }[]
  freeze_hash: string | null
}

export interface ScoutLatest {
  asset_id: string
  available: boolean
  meta: ScoutMeta
  counts: ScoutCounts | null
  open_events: ScoutEvent[]
  recent: ScoutEvent[] // newest first
}
