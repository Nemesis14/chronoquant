// Port of src/ui/components/charts.py::prediction_price_figure
// 3 rows: long pct (20%) / candles (60%) / short pct (20%); thresholds, signal markers,
// active trade overlay, focus window, optional Impulse Scout layer (t943, lib/scout.ts).
// The figure is built on the client from /api/market/chart.
import { lazy, Suspense, useMemo } from 'react'
import type { Annotation, Data, Layout, Shape } from 'plotly.js'
import type { Bar, ChartResponse, ScoutEvent } from '../../types/api'
import { COLORS } from '../../lib/format'
import { eventsInRange, scoutOverlay } from '../../lib/scout'

const PlotlyFigure = lazy(() => import('./PlotlyFigure'))

const MIN = 60_000
const CHART_H = 480

interface Candle { t: number; o: number; h: number; l: number; c: number }

function resample(bars: Bar[], minutes: number): Candle[] {
  const out: Candle[] = []
  const step = minutes * MIN
  let cur: Candle | null = null
  for (const b of bars) {
    if (b.o === null || b.h === null || b.l === null || b.c === null) continue
    const ms = Date.parse(b.t)
    const bucket = Math.floor(ms / step) * step
    if (!cur || cur.t !== bucket) {
      cur = { t: bucket, o: b.o, h: b.h, l: b.l, c: b.c }
      out.push(cur)
    } else {
      cur.h = Math.max(cur.h, b.h)
      cur.l = Math.min(cur.l, b.l)
      cur.c = b.c
    }
  }
  return out
}

const iso = (ms: number) => new Date(ms).toISOString()

export function buildFigure(
  chart: ChartResponse,
  scout: ScoutEvent[] | null = null,
  selectedId: string | null = null,
  /** false = data-only asset (no quant model, t946): candles only, no prediction rows */
  quant = true,
): { data: Data[]; layout: Partial<Layout> } {
  const { bars, thresholds, active_trade: trade, focus_hours: focus } = chart
  const x = bars.map((b) => b.t)
  const data: Data[] = []

  // row 1: long prediction (percentile)
  if (quant) data.push({
    type: 'scatter', mode: 'lines', x, y: bars.map((b) => b.long_pct),
    line: { color: COLORS.blue, width: 1.9 }, xaxis: 'x', yaxis: 'y',
    hovertemplate: 'long pct %{y:.3f}<extra></extra>',
  })
  // row 2: candles
  const minutes = focus > 8 ? 5 : focus > 4 ? 2 : 1
  const candles = minutes === 1
    ? bars.filter((b) => b.o !== null && b.h !== null && b.l !== null && b.c !== null)
        .map((b) => ({ t: Date.parse(b.t), o: b.o!, h: b.h!, l: b.l!, c: b.c! }))
    : resample(bars, minutes)
  data.push({
    type: 'candlestick', x: candles.map((c) => iso(c.t)),
    open: candles.map((c) => c.o), high: candles.map((c) => c.h),
    low: candles.map((c) => c.l), close: candles.map((c) => c.c),
    increasing: { line: { color: COLORS.green }, fillcolor: COLORS.green },
    decreasing: { line: { color: COLORS.red }, fillcolor: COLORS.red },
    xaxis: 'x', yaxis: 'y2', name: 'OHLC',
  })
  // row 3: short prediction
  if (quant) data.push({
    type: 'scatter', mode: 'lines', x, y: bars.map((b) => b.short_pct),
    line: { color: COLORS.red, width: 1.9 }, xaxis: 'x', yaxis: 'y3',
    hovertemplate: 'short pct %{y:.3f}<extra></extra>',
  })

  const lastMs = bars.length ? Date.parse(bars[bars.length - 1].t) : Date.now()
  const xStart = iso(lastMs - focus * 60 * MIN)
  const xEnd = iso(lastMs + 2 * MIN)

  // Scout events overlapping the focus window (their P* / stop also widen the price axis)
  const scoutEvents = scout ? eventsInRange(scout, lastMs - focus * 60 * MIN, lastMs + 2 * MIN) : []
  const scoutLevels = scoutEvents.flatMap((e) => [e.p_star, e.stop, e.low_t0].filter((v): v is number => v !== null))

  // price axis range (focus window + visible Scout levels), padded 6%
  const lows = [...candles.map((c) => c.l), ...scoutLevels]
  const highs = [...candles.map((c) => c.h), ...scoutLevels]
  let y2: [number, number] | undefined
  if (lows.length) {
    const lo = Math.min(...lows)
    const hi = Math.max(...highs)
    const pad = hi !== lo ? (hi - lo) * 0.06 : 0
    y2 = [lo - pad, hi + pad]
  }

  // signal markers: a Long jelek az ar-panel aljan, a Short jelek a tetejen (nem a gyertyakra rakva)
  const span = y2 ? y2[1] - y2[0] : 0
  const longSig = thresholds.long_entry === null ? [] :
    bars.filter((b) => b.long_pct !== null && b.long_pct >= thresholds.long_entry!)
  const shortSig = thresholds.short_entry === null ? [] :
    bars.filter((b) => b.short_pct !== null && b.short_pct >= thresholds.short_entry!)
  const longY = (b: Bar) => (y2 ? y2[0] + span * 0.025 : b.l)
  const shortY = (b: Bar) => (y2 ? y2[1] - span * 0.025 : b.h)
  if (longSig.length) {
    data.push({
      type: 'scatter', mode: 'markers', x: longSig.map((b) => b.t), y: longSig.map(longY),
      marker: { symbol: 'triangle-up', color: COLORS.green, size: 8 }, xaxis: 'x', yaxis: 'y2',
      hovertemplate: 'LONG jel<extra></extra>',
    })
  }
  if (shortSig.length) {
    data.push({
      type: 'scatter', mode: 'markers', x: shortSig.map((b) => b.t), y: shortSig.map(shortY),
      marker: { symbol: 'triangle-down', color: COLORS.red, size: 8 }, xaxis: 'x', yaxis: 'y2',
      hovertemplate: 'SHORT jel<extra></extra>',
    })
  }

  const shapes: Partial<Shape>[] = []
  const annotations: Partial<Annotation>[] = []
  if (scoutEvents.length) {
    const ov = scoutOverlay(scoutEvents, { xaxis: 'x', yaxis: 'y2', selectedId })
    data.push(...ov.data)
    shapes.push(...ov.shapes)
    annotations.push(...ov.annotations)
  }
  const hline = (yref: string, y: number, color: string, dash = 'dash'): Partial<Shape> => ({
    type: 'line', xref: 'paper', x0: 0, x1: 1, yref: yref as Shape['yref'], y0: y, y1: y,
    line: { color, width: 1.2, dash: dash as 'dash' },
  })
  if (thresholds.long_entry !== null) {
    shapes.push(hline('y', thresholds.long_entry, COLORS.green))
    annotations.push({
      xref: 'paper', yref: 'y domain', x: 0.01, y: 0.97, xanchor: 'left', yanchor: 'top', showarrow: false,
      text: `entry ${thresholds.long_entry.toFixed(3)}`, font: { color: COLORS.green, size: 11 },
      bgcolor: 'rgba(17,20,24,0.88)', bordercolor: COLORS.green, borderwidth: 1, borderpad: 3,
    })
  }
  if (thresholds.short_entry !== null) {
    shapes.push(hline('y3', thresholds.short_entry, COLORS.green))
    annotations.push({
      xref: 'paper', yref: 'y3 domain', x: 0.01, y: 0.97, xanchor: 'left', yanchor: 'top', showarrow: false,
      text: `entry ${thresholds.short_entry.toFixed(3)}`, font: { color: COLORS.green, size: 11 },
      bgcolor: 'rgba(17,20,24,0.88)', bordercolor: COLORS.green, borderwidth: 1, borderpad: 3,
    })
  }
  if (!bars.length) {
    annotations.push({ xref: 'paper', yref: 'paper', x: 0.5, y: 0.5, showarrow: false,
      text: 'Nincs adat', font: { color: COLORS.muted, size: 13 } })
  }
  if (trade) {
    if (trade.entry_price !== null) shapes.push(hline('y2', trade.entry_price, COLORS.gold))
    if (trade.stop_loss !== null) shapes.push(hline('y2', trade.stop_loss, COLORS.red))
    if (trade.take_profit !== null) shapes.push(hline('y2', trade.take_profit, COLORS.green))
  }

  const axisY = (domain: [number, number], title: string, color: string, extra: object = {}) => ({
    domain, side: 'right' as const, title: { text: title, font: { color, size: 11 } },
    tickfont: { color: COLORS.muted, size: 11 }, gridcolor: COLORS.grid, linecolor: COLORS.grid,
    zeroline: false, ...extra,
  })

  const layout: Partial<Layout> = {
    height: CHART_H,
    hovermode: 'x unified',
    dragmode: 'pan',
    uirevision: `cq-price-${chart.window}`,
    xaxis: {
      anchor: quant ? 'y3' : 'y2', range: [xStart, xEnd], rangeslider: { visible: false },
      gridcolor: COLORS.grid, linecolor: COLORS.grid, tickfont: { color: COLORS.muted, size: 11 },
      tickformat: '%H:%M<br>%m-%d', nticks: 9,
      showspikes: true, spikemode: 'across', spikesnap: 'cursor', spikedash: 'dot', spikecolor: COLORS.muted,
    },
    yaxis2: axisY(quant ? [0.22, 0.8] : [0, 1], '', COLORS.muted, { range: y2, tickformat: ',.2f' }),
    ...(quant ? {
      yaxis: axisY([0.82, 1], 'Long', COLORS.blue, { range: [-0.02, 1.02], tickformat: '.2f' }),
      yaxis3: axisY([0, 0.2], 'Short', '#e05252', { range: [-0.02, 1.02], tickformat: '.2f' }),
    } : {}),
    shapes,
    annotations,
  }
  return { data, layout }
}

export default function PredictionPriceChart({
  chart,
  scout = null,
  selectedId = null,
  quant = true,
}: {
  chart: ChartResponse
  /** false = data-only asset: candles only (no long/short prediction rows) */
  quant?: boolean
  /** Impulse Scout events to draw (null = layer off) */
  scout?: ScoutEvent[] | null
  selectedId?: string | null
}) {
  const fig = useMemo(() => buildFigure(chart, scout, selectedId, quant), [chart, scout, selectedId, quant])
  return (
    <Suspense fallback={<div className="h-[480px] flex items-center justify-center text-xs text-cq-muted">Grafikon betöltése…</div>}>
      <PlotlyFigure data={fig.data} layout={fig.layout} height={CHART_H} />
    </Suspense>
  )
}
