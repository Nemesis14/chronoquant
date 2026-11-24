// Impulse Scout event focus chart (epic_074 / t943): candles around one event
// ([t0 - 2h, label_end + 1h], 5m) with the Scout layer. Opened from the Scout panel, so events
// older than the dashboard's 24h window are also viewable. Data: /api/market/candles?end=.
import { lazy, Suspense, useMemo } from 'react'
import type { Data, Layout, Shape } from 'plotly.js'
import { getCandles } from '../../api/dashboard'
import { COLORS } from '../../lib/format'
import { scoutOverlay } from '../../lib/scout'
import { usePolling } from '../../lib/usePolling'
import type { CandlesResponse, ScoutEvent } from '../../types/api'
import { Muted, PollNote, Spinner } from '../dashboard/ui'

const PlotlyFigure = lazy(() => import('./PlotlyFigure'))

const H = 360
const HOUR = 3_600_000
const PRE_H = 2
const POST_H = 1

function eventSpan(e: ScoutEvent): { start: number; end: number } {
  const end = Date.parse(e.label_end ?? e.t_c) + POST_H * HOUR
  return { start: Date.parse(e.t0) - PRE_H * HOUR, end }
}

function buildFigure(c: CandlesResponse, e: ScoutEvent): { data: Data[]; layout: Partial<Layout> } {
  const { start, end } = eventSpan(e)
  const cs = c.candles.filter((k) => k.o !== null && k.h !== null && k.l !== null && k.c !== null)
  const data: Data[] = [{
    type: 'candlestick', x: cs.map((k) => k.t),
    open: cs.map((k) => k.o!), high: cs.map((k) => k.h!), low: cs.map((k) => k.l!), close: cs.map((k) => k.c!),
    increasing: { line: { color: COLORS.green }, fillcolor: COLORS.green },
    decreasing: { line: { color: COLORS.red }, fillcolor: COLORS.red },
    name: 'OHLC', xaxis: 'x', yaxis: 'y',
  }]
  const ov = scoutOverlay([e], { xaxis: 'x', yaxis: 'y', selectedId: e.event_id })
  data.push(...ov.data)
  const shapes: Partial<Shape>[] = [...ov.shapes]
  // "now" marker for an open event (the H window still runs into the future)
  if (e.status === 'open' && c.to) {
    shapes.push({
      type: 'line', xref: 'x', yref: 'paper', x0: c.to, x1: c.to, y0: 0, y1: 1,
      line: { color: COLORS.muted, width: 1, dash: 'dash' },
    })
    ov.annotations.push({
      xref: 'x', yref: 'paper', x: c.to, y: 1, xanchor: 'right', yanchor: 'top', showarrow: false,
      text: 'most', font: { color: COLORS.muted, size: 10 },
    })
  }
  const levels = [
    ...cs.map((k) => k.l!), ...cs.map((k) => k.h!),
    ...[e.p_star, e.stop, e.low_t0].filter((v): v is number => v !== null),
  ]
  const lo = Math.min(...levels)
  const hi = Math.max(...levels)
  const pad = hi > lo ? (hi - lo) * 0.08 : 1
  const layout: Partial<Layout> = {
    height: H,
    hovermode: 'closest',
    dragmode: 'pan',
    uirevision: `cq-scout-${e.event_id}`,
    xaxis: {
      range: [new Date(start).toISOString(), new Date(end).toISOString()], rangeslider: { visible: false },
      gridcolor: COLORS.grid, linecolor: COLORS.grid, tickfont: { color: COLORS.muted, size: 11 },
      tickformat: '%H:%M<br>%m-%d', nticks: 9,
    },
    yaxis: {
      side: 'right', range: [lo - pad, hi + pad], tickformat: ',.2f',
      gridcolor: COLORS.grid, linecolor: COLORS.grid, tickfont: { color: COLORS.muted, size: 11 }, zeroline: false,
    },
    shapes,
    annotations: ov.annotations,
  }
  return { data, layout }
}

export default function ScoutEventChart({ asset, event }: { asset: string; event: ScoutEvent }) {
  const { start, end } = eventSpan(event)
  const hours = Math.min(24 * 14, Math.ceil((end - start) / HOUR))
  const endIso = new Date(end).toISOString().replace('.000Z', 'Z')
  // open event: refresh every minute (the window grows); resolved: fetch once
  const poll = usePolling(
    (s) => getCandles(asset, { hours, tf: '5m', end: endIso }, s),
    event.status === 'open' ? 60_000 : 0,
    [asset, event.event_id, event.status],
  )
  const fig = useMemo(() => (poll.data ? buildFigure(poll.data, event) : null), [poll.data, event])
  return (
    <div>
      {fig && poll.data && poll.data.candles.length > 0 ? (
        <Suspense fallback={<div className="h-[360px] flex items-center justify-center text-xs text-cq-muted">Grafikon betöltése…</div>}>
          <PlotlyFigure data={fig.data} layout={fig.layout} height={H} />
        </Suspense>
      ) : (
        <div className="h-[360px] flex items-center justify-center gap-2">
          {poll.loading ? <><Spinner /><Muted>Gyertyák betöltése…</Muted></> : poll.data ? <Muted>Nincs gyertya az esemény ablakában.</Muted> : null}
        </div>
      )}
      <PollNote poll={poll} what="Scout esemény gyertyák" />
    </div>
  )
}
