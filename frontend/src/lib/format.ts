export const COLORS = {
  bg: '#0b0e11',
  panel: '#111418',
  grid: '#2b3139',
  text: '#eaecef',
  muted: '#848e9c',
  green: '#0ecb81',
  red: '#f6465d',
  gold: '#f0b90b',
  blue: '#5b8af5',
}

export const DASH = '—'

export function num(v: number | null | undefined, digits = 4): string {
  if (v === null || v === undefined || Number.isNaN(v)) return DASH
  return v.toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits })
}

export function signed(v: number | null | undefined, digits = 4): string {
  if (v === null || v === undefined || Number.isNaN(v)) return DASH
  return `${v > 0 ? '+' : ''}${v.toFixed(digits)}`
}

export function pct(v: number | null | undefined, digits = 1): string {
  if (v === null || v === undefined || Number.isNaN(v)) return DASH
  return `${(v * 100).toFixed(digits)}%`
}

/** "2026-06-14T10:21:00Z" or "2026-06-14 10:21:00" -> "06-14 10:21" (UTC as delivered). */
export function shortTime(v: string | null | undefined): string {
  if (!v) return DASH
  const s = String(v).replace('T', ' ')
  return s.length >= 16 ? s.slice(5, 16) : s
}

export function fullTime(v: string | null | undefined): string {
  if (!v) return DASH
  return String(v).replace('T', ' ').replace('Z', '').slice(0, 19)
}

export function pnlColor(v: number | null | undefined): string {
  if (v === null || v === undefined || v === 0) return 'text-cq-muted'
  return v > 0 ? 'text-cq-green' : 'text-cq-red'
}

export function sideColor(side: string | null | undefined): string {
  const s = (side ?? '').toUpperCase()
  if (s === 'LONG' || s === 'BUY') return 'text-cq-green'
  if (s === 'SHORT' || s === 'SELL') return 'text-cq-red'
  return 'text-cq-muted'
}

export function ago(iso: string | null | undefined): string {
  if (!iso) return DASH
  const t = Date.parse(iso)
  if (Number.isNaN(t)) return DASH
  const s = Math.max(0, Math.round((Date.now() - t) / 1000))
  if (s < 90) return `${s} mp`
  if (s < 5400) return `${Math.round(s / 60)} perc`
  if (s < 172800) return `${Math.round(s / 3600)} óra`
  return `${Math.round(s / 86400)} nap`
}

export function errMessage(e: unknown): string {
  return e instanceof Error ? e.message : String(e)
}
