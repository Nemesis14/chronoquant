import { api } from './client'
import type {
  ActivePosition, ActiveStrategies, AutoSync, BinanceFill, ChartResponse, ClosedTrade, EquityResponse,
  Job, LogLine, Page, RegistryActive, Row, Status, StrategyMetrics, WorkerInfo,
} from '../types/api'

export const getStatus = (s?: AbortSignal) => api.get<Status>('/api/status', undefined, s)
export const getChart = (window: string, s?: AbortSignal) =>
  api.get<ChartResponse>('/api/market/chart', { window }, s)
export const getActiveTrade = (s?: AbortSignal) => api.get<ActivePosition>('/api/trades/active', undefined, s)
export const getBinanceFills = (limit: number, s?: AbortSignal) =>
  api.get<{ items: BinanceFill[]; source: string }>('/api/trades/binance', { limit }, s)
export const getClosedTrades = (limit: number, s?: AbortSignal) =>
  api.get<Page<ClosedTrade>>('/api/trades/closed', { limit }, s)
export const getJournal = (
  kind: 'positions' | 'orders' | 'signals' | 'errors' | 'runs',
  limit: number,
  s?: AbortSignal,
) => api.get<Page<Row>>(`/api/trades/journal/${kind}`, { limit }, s)
export const getEquity = (s?: AbortSignal) => api.get<EquityResponse>('/api/equity', { direction: 'both' }, s)
export const getEquitySummary = (s?: AbortSignal) =>
  api.get<Record<'long' | 'short', StrategyMetrics>>('/api/equity/summary', undefined, s)
export const getActiveStrategies = (s?: AbortSignal) => api.get<ActiveStrategies>('/api/strat/active', undefined, s)
export const getRegistryActive = (s?: AbortSignal) => api.get<RegistryActive>('/api/registry/active', undefined, s)
export const getLogs = (source: 'api' | 'worker', lines: number, s?: AbortSignal) =>
  api.get<{ lines: LogLine[] }>(`/api/logs/${source}`, { lines }, s)

// jobs
export const listJobs = (limit = 10, s?: AbortSignal) => api.get<{ items: Job[] }>('/api/jobs', { limit }, s)
export const getWorker = (s?: AbortSignal) => api.get<WorkerInfo>('/api/jobs/worker', undefined, s)
export const getAutoSync = (s?: AbortSignal) => api.get<AutoSync>('/api/jobs/auto-sync', undefined, s)
export const setAutoSync = (enabled: boolean) => api.put<AutoSync>('/api/jobs/auto-sync', { enabled })
export const enqueueSync = (kind: 'sync_full' | 'sync_predictions' = 'sync_full') =>
  api.post<{ job: Job; deduplicated: boolean }>('/api/jobs', { kind, params: {} })
export const cancelJob = (id: string) => api.del<Job>(`/api/jobs/${encodeURIComponent(id)}`)
