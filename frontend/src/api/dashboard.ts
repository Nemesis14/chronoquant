import { api } from './client'
import type {
  ActivePosition, ActiveStrategies, AutoSync, BinanceFill, CandlesResponse, ChartResponse, ClosedTrade, EquityResponse,
  AssetList, Job, LogLine, Page, RegistryActive, Row, Status, StrategyMetrics, WorkerInfo,
} from '../types/api'

export const getAssets = (s?: AbortSignal) => api.get<AssetList>('/api/assets', undefined, s)
export const getStatus = (asset: string, s?: AbortSignal) => api.get<Status>('/api/status', { asset }, s)
export const getChart = (asset: string, window: string, s?: AbortSignal) =>
  api.get<ChartResponse>('/api/market/chart', { window, asset }, s)
/** OHLCV candles over [end - hours, end] (end: ISO-Z, capped at the latest bar; omitted = latest). */
export const getCandles = (
  asset: string, opts: { hours: number; tf: '1m' | '5m' | '15m'; end?: string }, s?: AbortSignal,
) => api.get<CandlesResponse>('/api/market/candles', { asset, hours: opts.hours, tf: opts.tf, end: opts.end }, s)
export const getActiveTrade = (asset: string, s?: AbortSignal) =>
  api.get<ActivePosition>('/api/trades/active', { asset }, s)
export const getBinanceFills = (asset: string, limit: number, s?: AbortSignal) =>
  api.get<{ items: BinanceFill[]; source: string }>('/api/trades/binance', { limit, asset }, s)
export const getClosedTrades = (asset: string, limit: number, s?: AbortSignal) =>
  api.get<Page<ClosedTrade>>('/api/trades/closed', { limit, asset }, s)
export const getJournal = (
  asset: string,
  kind: 'positions' | 'orders' | 'signals' | 'errors' | 'runs',
  limit: number,
  s?: AbortSignal,
) => api.get<Page<Row>>(`/api/trades/journal/${kind}`, { limit, asset }, s)
export const getEquity = (asset: string, s?: AbortSignal) =>
  api.get<EquityResponse>('/api/equity', { direction: 'both', asset }, s)
export const getEquitySummary = (asset: string, s?: AbortSignal) =>
  api.get<Record<'long' | 'short', StrategyMetrics>>('/api/equity/summary', { asset }, s)
export const getActiveStrategies = (asset: string, s?: AbortSignal) =>
  api.get<ActiveStrategies>('/api/strat/active', { asset }, s)
export const getRegistryActive = (asset: string, s?: AbortSignal) =>
  api.get<RegistryActive>('/api/registry/active', { asset }, s)
export const getLogs = (source: 'api' | 'worker', lines: number, s?: AbortSignal) =>
  api.get<{ lines: LogLine[] }>(`/api/logs/${source}`, { lines }, s)

// jobs
export const listJobs = (limit = 10, s?: AbortSignal) => api.get<{ items: Job[] }>('/api/jobs', { limit }, s)
export const getWorker = (s?: AbortSignal) => api.get<WorkerInfo>('/api/jobs/worker', undefined, s)
export const getAutoSync = (s?: AbortSignal) => api.get<AutoSync>('/api/jobs/auto-sync', undefined, s)
export const setAutoSync = (enabled: boolean) => api.put<AutoSync>('/api/jobs/auto-sync', { enabled })
export const enqueueSync = (asset: string, kind: 'sync_full' | 'sync_predictions' = 'sync_full') =>
  api.post<{ job: Job; deduplicated: boolean }>('/api/jobs', { kind, params: { asset_id: asset } })
export const cancelJob = (id: string) => api.del<Job>(`/api/jobs/${encodeURIComponent(id)}`)
