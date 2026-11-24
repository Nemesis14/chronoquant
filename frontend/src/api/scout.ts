// Impulse Scout (epic_074 / t942): read-only forward-log API. Experimental, NO-GO: not a trading signal.
import { api } from './client'
import type { Job, ScoutLatest, ScoutSignals } from '../types/api'

/** Newest `limit` events with t0 >= since (ISO-Z), ascending by t0; `available: false` without a table (BCH). */
export const getScoutSignals = (
  asset: string,
  opts: { since?: string; limit?: number } = {},
  s?: AbortSignal,
) => api.get<ScoutSignals>('/api/scout/signals', { asset, since: opts.since, limit: opts.limit }, s)

/** Counters, open events, newest `recent` events (newest first) + artifact meta (thresholds, live_start, experimental). */
export const getScoutLatest = (asset: string, recent = 5, s?: AbortSignal) =>
  api.get<ScoutLatest>('/api/scout/latest', { asset, recent }, s)

/** Manual Scout run (worker job `sync_scout`, Scout assets only; 400 for others). */
export const enqueueScoutSync = (asset: string) =>
  api.post<{ job: Job; deduplicated: boolean }>('/api/jobs', { kind: 'sync_scout', params: { asset_id: asset } })
