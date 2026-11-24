// Impulse Scout (epic_074 / t943): shared labels, colors and the Plotly price-chart overlay.
// Experimental, NO-GO model: the UI always marks it "Kísérleti – nem kereskedési jel".
import type { Annotation, Data, Shape } from 'plotly.js'
import type { ScoutEvent, ScoutLabel, ScoutOutcome, ScoutStatus } from '../types/api'
import { COLORS, pct } from './format'

/** Scout identity hue (t0 marker, connector, badges): violet, distinct from the quant model colors. */
export const SCOUT_COLOR = '#b37feb'

export const RULE_KEYS = ['default', 'r2_03'] as const
export const RULE_LABEL: Record<string, string> = {
  default: 'Alap (P* / mélypont)',
  r2_03: 'R2_03 (+3σ / −1,5σ)',
}

export const CLASS_LABEL: Record<ScoutLabel, string> = {
  impulse: 'impulzus',
  false_start: 'hamis indulás',
  sideways: 'oldalazás',
}
export const CLASS_COLOR: Record<ScoutLabel, string> = {
  impulse: COLORS.green,
  false_start: COLORS.red,
  sideways: COLORS.muted,
}
export const OUTCOME_LABEL: Record<ScoutOutcome, string> = {
  tp: 'TP',
  sl: 'SL',
  timeout: 'lejárt',
  invalid: 'érvénytelen',
}
/** Text glyph next to the color so the outcome is never color-alone. */
export const OUTCOME_ICON: Record<ScoutOutcome, string> = { tp: '✓', sl: '✗', timeout: '◷', invalid: '∅' }
export const OUTCOME_CLASS: Record<ScoutOutcome, string> = {
  tp: 'text-cq-green',
  sl: 'text-cq-red',
  timeout: 'text-cq-muted',
  invalid: 'text-cq-muted',
}
export const STATUS_LABEL: Record<ScoutStatus, string> = {
  open: 'nyitott',
  resolved: 'lezárt',
  invalid: 'érvénytelen',
}

/** Event state color: open = blue, resolved = realised class, invalid / unknown = muted. */
export function eventColor(e: ScoutEvent): string {
  if (e.status === 'open') return COLORS.blue
  if (e.status === 'invalid') return COLORS.muted
  if (e.label_class) return CLASS_COLOR[e.label_class]
  const o = e.rules.default?.outcome
  return o === 'tp' ? COLORS.green : o === 'sl' ? COLORS.red : COLORS.muted
}

/** Short state text: "nyitott" / "impulzus" / "érvénytelen". */
export function eventStateText(e: ScoutEvent): string {
  if (e.status !== 'resolved') return STATUS_LABEL[e.status]
  return e.label_class ? CLASS_LABEL[e.label_class] : STATUS_LABEL.resolved
}

export function percentileText(p: number | null | undefined): string {
  return p === null || p === undefined ? '—' : `${p.toFixed(0)}. pc`
}

/** "+372 bp" */
export function bps(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return '—'
  return `${v > 0 ? '+' : ''}${v.toFixed(0)} bp`
}

/** Price distance from entry in % ("+2.7%"). */
export function distPct(price: number | null, entry: number | null): string {
  if (price === null || entry === null || entry === 0) return ''
  const d = (price / entry - 1) * 100
  return `${d > 0 ? '+' : ''}${d.toFixed(2)}%`
}

export function topBadge(e: ScoutEvent): 'top10' | 'top20' | null {
  if (e.top10) return 'top10'
  if (e.top20) return 'top20'
  return null
}

const ms = (iso: string | null) => (iso ? Date.parse(iso) : NaN)

/** Events whose [t0, label_end] interval overlaps [fromMs, toMs]. */
export function eventsInRange(events: ScoutEvent[], fromMs: number, toMs: number): ScoutEvent[] {
  return events.filter((e) => {
    const a = ms(e.t0)
    const b = Number.isNaN(ms(e.label_end)) ? ms(e.t_c) : ms(e.label_end)
    return b >= fromMs && a <= toMs
  })
}

/**
 * Scout layer on a price axis: t0 low (violet ▲), t_c confirmation (● in state color),
 * dotted P* / stop over the H window (t_c → label_end) with a faint state-colored band,
 * and a small label "Scout P(imp) 16.0% · 63. pc [✓ impulzus]".
 */
export function scoutOverlay(
  events: ScoutEvent[],
  opts: { xaxis?: string; yaxis?: string; selectedId?: string | null; labels?: boolean } = {},
): { data: Data[]; shapes: Partial<Shape>[]; annotations: Partial<Annotation>[] } {
  const xaxis = opts.xaxis ?? 'x'
  const yaxis = opts.yaxis ?? 'y2'
  const xref = xaxis as Shape['xref']
  const yref = yaxis as Shape['yref']
  const data: Data[] = []
  const shapes: Partial<Shape>[] = []
  const annotations: Partial<Annotation>[] = []
  if (!events.length) return { data, shapes, annotations }

  for (const e of events) {
    const color = eventColor(e)
    const sel = opts.selectedId === e.event_id
    const end = e.label_end ?? e.t_c
    if (e.p_star !== null && e.stop !== null) {
      shapes.push({
        type: 'rect', xref, yref, x0: e.t_c, x1: end, y0: e.stop, y1: e.p_star, layer: 'below',
        fillcolor: color, opacity: e.status === 'open' ? 0.1 : 0.07, line: { width: 0 },
      })
    }
    if (e.p_star !== null) {
      shapes.push({
        type: 'line', xref, yref, x0: e.t_c, x1: end, y0: e.p_star, y1: e.p_star,
        line: { color: COLORS.green, width: sel ? 2 : 1.4, dash: 'dot' },
      })
    }
    if (e.stop !== null) {
      shapes.push({
        type: 'line', xref, yref, x0: e.t_c, x1: end, y0: e.stop, y1: e.stop,
        line: { color: COLORS.red, width: sel ? 2 : 1.4, dash: 'dot' },
      })
    }
    if (e.low_t0 !== null && e.close_tc !== null) {
      shapes.push({
        type: 'line', xref, yref, x0: e.t0, x1: e.t_c, y0: e.low_t0, y1: e.close_tc,
        line: { color: SCOUT_COLOR, width: 1, dash: 'dot' },
      })
    }
    if (opts.labels !== false && e.p_star !== null) {
      const o = e.rules.default?.outcome ?? null
      const state = e.status === 'open'
        ? 'nyitott'
        : `${o ? `${OUTCOME_ICON[o]} ` : ''}${eventStateText(e)}`
      const top = topBadge(e)
      annotations.push({
        xref: xaxis as Annotation['xref'], yref: yaxis as Annotation['yref'], x: e.t_c, y: e.p_star, xanchor: 'left', yanchor: 'bottom', showarrow: false,
        text: `Scout ${pct(e.p_impulse)} · ${percentileText(e.oof_percentile)}${top ? ` · ${top === 'top10' ? 'top 10%' : 'top 20%'}` : ''} · ${state}`,
        font: { color: COLORS.text, size: 10 },
        bgcolor: 'rgba(17,20,24,0.88)', bordercolor: color, borderwidth: sel ? 2 : 1, borderpad: 2,
      })
    }
  }

  const hover = (e: ScoutEvent, what: string) => {
    const d = e.rules.default
    const out = e.status === 'open'
      ? 'nyitott (kimenet: ' + (e.label_end ?? '—').replace('T', ' ').slice(5, 16) + ')'
      : `${eventStateText(e)}${d?.outcome ? ` · alap ${OUTCOME_LABEL[d.outcome]} ${bps(d.net_bps)}` : ''}`
    return `<b>Scout ${what}</b><br>P(impulzus) ${pct(e.p_impulse)} · ${percentileText(e.oof_percentile)}<br>${out}<br><i>kísérleti – nem kereskedési jel</i>`
  }
  const withLow = events.filter((e) => e.low_t0 !== null)
  data.push({
    type: 'scatter', mode: 'markers', xaxis, yaxis, name: 'Scout t0',
    x: withLow.map((e) => e.t0), y: withLow.map((e) => e.low_t0),
    marker: { symbol: 'triangle-up', color: SCOUT_COLOR, size: 11, line: { color: COLORS.bg, width: 1.5 } },
    text: withLow.map((e) => hover(e, 'mélypont (t0)')),
    hovertemplate: '%{text}<extra></extra>',
  })
  const withTc = events.filter((e) => e.close_tc !== null)
  data.push({
    type: 'scatter', mode: 'markers', xaxis, yaxis, name: 'Scout t_c',
    x: withTc.map((e) => e.t_c), y: withTc.map((e) => e.close_tc),
    marker: {
      symbol: 'circle', size: 10, color: withTc.map(eventColor),
      line: { color: withTc.map((e) => (opts.selectedId === e.event_id ? COLORS.text : COLORS.bg)), width: 1.5 },
    },
    text: withTc.map((e) => hover(e, 'megerősítés (t_c)')),
    hovertemplate: '%{text}<extra></extra>',
  })
  return { data, shapes, annotations }
}
