// Impulse Scout panel (epic_074 / t943): experimental forward-log of the frozen Scout model.
// Open event(s) highlighted, recent events (click = focus chart), 3-class probability bar,
// OOF percentile + top-20/10% badge, trade plan for both exit rules, outcome + net bp.
import { AlertTriangle } from 'lucide-react'
import { ApiError } from '../../api/client'
import { ago, DASH, num, pct, shortTime } from '../../lib/format'
import {
  bps, CLASS_COLOR, CLASS_LABEL, distPct, eventColor, eventStateText, OUTCOME_CLASS, OUTCOME_ICON, OUTCOME_LABEL,
  percentileText, RULE_KEYS, RULE_LABEL, SCOUT_COLOR, topBadge,
} from '../../lib/scout'
import type { PollState } from '../../lib/usePolling'
import type { ScoutEvent, ScoutLabel, ScoutLatest, ScoutRuleResult } from '../../types/api'
import { Card, Muted, PollNote, Spinner } from './ui'

const LIST_N = 12

export function ExperimentalBadge({ compact = false }: { compact?: boolean }) {
  return (
    <span
      className={`inline-flex items-center gap-1 rounded border border-cq-gold bg-cq-gold/15 text-cq-gold font-bold uppercase tracking-wide ${compact ? 'px-1.5 py-0 text-[10px]' : 'px-2 py-1 text-[11px]'}`}
      title="NO-GO státuszú kutatási modell: nem kereskedési jel, nem kerül a quant modellbe"
    >
      <AlertTriangle size={compact ? 11 : 13} /> Kísérleti – nem kereskedési jel
    </span>
  )
}

function TopBadge({ e }: { e: ScoutEvent }) {
  const t = topBadge(e)
  if (!t) return null
  return (
    <span
      className="rounded px-1 text-[10px] font-bold border"
      style={{ color: SCOUT_COLOR, borderColor: SCOUT_COLOR }}
      title="A train OOF score-eloszlás felső 20% / 10%-ában"
    >
      {t === 'top10' ? 'TOP 10%' : 'TOP 20%'}
    </span>
  )
}

/** State chip: colored dot + text (never color-alone). */
function StateChip({ e }: { e: ScoutEvent }) {
  return (
    <span className="inline-flex items-center gap-1">
      <span className="inline-block w-2 h-2 rounded-full" style={{ background: eventColor(e) }} />
      <span className={e.status === 'open' ? 'text-cq-blue font-semibold' : ''}>{eventStateText(e)}</span>
    </span>
  )
}

const ORDER: ScoutLabel[] = ['impulse', 'sideways', 'false_start']

/** 100% stacked bar of the 3 class probabilities + labelled values below. */
function ProbBar({ e }: { e: ScoutEvent }) {
  const p: Record<ScoutLabel, number | null> = {
    impulse: e.p_impulse, sideways: e.p_sideways, false_start: e.p_false_start,
  }
  if (e.p_impulse === null) return <Muted>Nem score-olható esemény (hiányzó feature).</Muted>
  return (
    <div>
      <div className="flex h-2.5 gap-[2px] rounded overflow-hidden bg-cq-bg">
        {ORDER.map((k) => (
          <div
            key={k}
            style={{ width: `${(p[k] ?? 0) * 100}%`, background: CLASS_COLOR[k], opacity: k === 'sideways' ? 0.6 : 1 }}
            title={`${CLASS_LABEL[k]} ${pct(p[k])}`}
          />
        ))}
      </div>
      <div className="flex justify-between text-[11px] mt-1">
        {ORDER.map((k) => (
          <span key={k} className="flex items-center gap-1">
            <span className="inline-block w-2 h-2 rounded-sm" style={{ background: CLASS_COLOR[k], opacity: k === 'sideways' ? 0.6 : 1 }} />
            <span className="text-cq-muted">{CLASS_LABEL[k]}</span>
            <span className="font-semibold">{pct(p[k])}</span>
          </span>
        ))}
      </div>
    </div>
  )
}

function Outcome({ r }: { r: ScoutRuleResult | undefined }) {
  if (!r?.outcome) return <span className="text-cq-muted">{DASH}</span>
  return (
    <span className={OUTCOME_CLASS[r.outcome]}>
      {OUTCOME_ICON[r.outcome]} {OUTCOME_LABEL[r.outcome]}
    </span>
  )
}

function NetBp({ v }: { v: number | null | undefined }) {
  if (v === null || v === undefined) return <span className="text-cq-muted">{DASH}</span>
  return <span className={v > 0 ? 'text-cq-green' : v < 0 ? 'text-cq-red' : 'text-cq-muted'}>{bps(v)}</span>
}

/** Trade plan (entry, TP, SL with % distance) + outcome per exit rule. */
function PlanTable({ e }: { e: ScoutEvent }) {
  const entry = e.entry ?? e.close_tc
  return (
    <table className="w-full text-[11px]">
      <thead>
        <tr className="text-cq-muted border-b border-cq-grid">
          <th className="text-left font-normal py-0.5">Szabály</th>
          <th className="text-right font-normal">TP</th>
          <th className="text-right font-normal">SL</th>
          <th className="text-right font-normal">Kimenet</th>
          <th className="text-right font-normal">Nettó</th>
        </tr>
      </thead>
      <tbody>
        {RULE_KEYS.map((k) => {
          const r = e.rules[k]
          return (
            <tr key={k} className="border-b border-cq-grid/40 last:border-0">
              <td className="py-1 pr-1">{RULE_LABEL[k]}</td>
              <td className="text-right whitespace-nowrap">
                <span className="text-cq-green">{num(r?.tp ?? null, 2)}</span>
                <div className="text-cq-muted text-[10px]">{distPct(r?.tp ?? null, entry)}</div>
              </td>
              <td className="text-right whitespace-nowrap">
                <span className="text-cq-red">{num(r?.sl ?? null, 2)}</span>
                <div className="text-cq-muted text-[10px]">{distPct(r?.sl ?? null, entry)}</div>
              </td>
              <td className="text-right whitespace-nowrap"><Outcome r={r} /></td>
              <td className="text-right whitespace-nowrap"><NetBp v={r?.net_bps} /></td>
            </tr>
          )
        })}
      </tbody>
    </table>
  )
}

function untilText(iso: string | null): string {
  if (!iso) return DASH
  const d = Date.parse(iso) - Date.now()
  if (d <= 0) return 'lezárulás folyamatban'
  const m = Math.round(d / 60_000)
  return m < 90 ? `${m} perc múlva` : `${(m / 60).toFixed(1)} óra múlva`
}

/** Full detail of one event (open-event highlight or the selected row). */
function EventDetail({ e, latest, onShow, shown }: {
  e: ScoutEvent
  latest: ScoutLatest
  onShow?: () => void
  shown?: boolean
}) {
  const open = e.status === 'open'
  const thr = latest.meta.thresholds
  return (
    <div
      className={`rounded border p-2 space-y-2 text-xs ${open ? 'border-cq-blue bg-cq-blue/10' : 'border-cq-grid bg-cq-bg/40'}`}
    >
      <div className="flex items-center gap-2 flex-wrap">
        {open ? (
          <span className="text-cq-blue font-bold">● NYITOTT esemény</span>
        ) : (
          <StateChip e={e} />
        )}
        <TopBadge e={e} />
        {e.backfilled && <span className="text-[10px] text-cq-muted" title="t_c az első éles írás előtt (utólag töltve)">backfill</span>}
        {onShow && (
          <button
            onClick={onShow}
            className={`ml-auto text-[11px] underline ${shown ? 'text-cq-text' : 'text-cq-muted hover:text-cq-text'}`}
          >
            {shown ? 'grafikon bezárása' : 'grafikonon'}
          </button>
        )}
      </div>
      <div className="flex items-baseline gap-3">
        <div>
          <div className="text-[10px] text-cq-muted uppercase">P(impulzus)</div>
          <div className="text-xl font-bold leading-none">{pct(e.p_impulse)}</div>
        </div>
        <div>
          <div className="text-[10px] text-cq-muted uppercase">Percentilis</div>
          <div className="text-base font-semibold leading-none">{percentileText(e.oof_percentile)}</div>
        </div>
        <div className="ml-auto text-right text-[11px] text-cq-muted leading-tight">
          <div>top-20%: ≥ {pct(thr.top20)}</div>
          <div>top-10%: ≥ {pct(thr.top10)}</div>
        </div>
      </div>
      <ProbBar e={e} />
      <div className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5 text-[11px]">
        <span className="text-cq-muted">Mélypont (t0)</span>
        <span className="text-right">{shortTime(e.t0)} · {num(e.low_t0, 2)}</span>
        <span className="text-cq-muted">Megerősítés (t_c)</span>
        <span className="text-right">{shortTime(e.t_c)} · {num(e.close_tc, 2)}</span>
        <span className="text-cq-muted">Entry</span>
        <span className="text-right">{num(e.entry, 2)}</span>
        <span className="text-cq-muted">Ablak vége</span>
        <span className="text-right">
          {shortTime(e.label_end)}{open ? ` (${untilText(e.label_end)})` : ''}
        </span>
      </div>
      <PlanTable e={e} />
    </div>
  )
}

export default function ScoutPanel({ poll, selectedId, onSelect }: {
  poll: PollState<ScoutLatest>
  selectedId: string | null
  onSelect: (id: string | null) => void
}) {
  const d = poll.data
  const notServed = poll.error instanceof ApiError && poll.error.status === 404 && !d
  const title = (
    <span className="flex items-center gap-2">
      <span className="inline-block w-2.5 h-2.5 rotate-45" style={{ background: SCOUT_COLOR }} />
      Impulse Scout
    </span>
  )

  if (notServed) {
    return (
      <Card title={title}>
        <Muted>A Scout API nem érhető el (az API újraindítása után aktív).</Muted>
      </Card>
    )
  }
  if (d && !d.available) {
    return (
      <Card title={title}>
        <Muted>Az Impulse Scout ehhez az asset-hez nem elérhető (modellen kívüli asset; Scout: {d.meta.train_assets.map((a) => a.replace('usdt', '').toUpperCase()).join(' / ')}).</Muted>
      </Card>
    )
  }

  const counts = d?.counts
  const list = d?.recent.slice(0, LIST_N) ?? []
  const openIds = new Set(d?.open_events.map((e) => e.event_id))
  const selected = d && selectedId && !openIds.has(selectedId)
    ? d.recent.find((e) => e.event_id === selectedId) ?? null
    : null

  return (
    <Card title={title} right={poll.refreshing && <Spinner />}>
      {d && (
        <div className="space-y-2">
          {(d.meta.experimental || d.meta.status.includes('no_go')) && (
            <div className="space-y-1">
              <ExperimentalBadge />
              <div className="text-[11px] text-cq-muted">
                NO-GO kutatási modell ({d.meta.model_id}, train OOF AUC {d.meta.oof_auc?.toFixed(3) ?? DASH}),
                befagyasztva; előre rögzített predikciók {d.meta.live_start?.slice(0, 10) ?? DASH}-tól. Nem része a quant modellnek.
              </div>
            </div>
          )}
          {counts && (
            <div className="text-[11px] text-cq-muted">
              {counts.total} esemény · <span className="text-cq-blue">{counts.open} nyitott</span> · {counts.resolved} lezárt
              {counts.invalid > 0 ? ` · ${counts.invalid} érvénytelen` : ''} · top-20%: {counts.top20}
              {' · '}frissítve {ago(counts.last_scored_at)} ezelőtt
            </div>
          )}

          {d.open_events.map((e) => (
            <EventDetail
              key={e.event_id}
              e={e}
              latest={d}
              shown={selectedId === e.event_id}
              onShow={() => onSelect(selectedId === e.event_id ? null : e.event_id)}
            />
          ))}
          {d.open_events.length === 0 && <Muted>Nincs nyitott esemény.</Muted>}

          <div>
            <div className="text-[11px] text-cq-muted mb-1">Legutóbbi események (kattintás: részletek + grafikon)</div>
            {list.length === 0 ? (
              <Muted>Még nincs esemény.</Muted>
            ) : (
              <div className="overflow-auto max-h-72">
                <table className="w-full text-[11px]">
                  <thead className="sticky top-0 bg-cq-panel">
                    <tr className="text-cq-muted border-b border-cq-grid">
                      <th className="text-left font-normal py-0.5">t_c</th>
                      <th className="text-right font-normal">P(imp)</th>
                      <th className="text-right font-normal">pc</th>
                      <th className="text-left font-normal pl-2">Kimenet</th>
                      <th className="text-right font-normal" title="Alap szabály nettó">Alap</th>
                      <th className="text-right font-normal" title="R2_03 szabály nettó">R2_03</th>
                    </tr>
                  </thead>
                  <tbody>
                    {list.map((e) => {
                      const sel = e.event_id === selectedId
                      return (
                        <tr
                          key={e.event_id}
                          onClick={() => onSelect(sel ? null : e.event_id)}
                          className={`cursor-pointer border-b border-cq-grid/40 ${sel ? 'bg-white/10' : 'hover:bg-white/5'} ${e.status === 'open' ? 'text-cq-blue' : ''}`}
                        >
                          <td className="py-1 whitespace-nowrap">{shortTime(e.t_c)}</td>
                          <td className="text-right whitespace-nowrap">
                            <span className="inline-flex items-center gap-1">
                              <TopBadge e={e} />
                              <span className="font-semibold">{pct(e.p_impulse)}</span>
                            </span>
                          </td>
                          <td className="text-right text-cq-muted">{e.oof_percentile === null ? DASH : e.oof_percentile.toFixed(0)}</td>
                          <td className="pl-2 whitespace-nowrap"><StateChip e={e} /></td>
                          <td className="text-right whitespace-nowrap"><NetBp v={e.rules.default?.net_bps} /></td>
                          <td className="text-right whitespace-nowrap"><NetBp v={e.rules.r2_03?.net_bps} /></td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          {selected && (
            <EventDetail e={selected} latest={d} shown onShow={() => onSelect(null)} />
          )}
          <div className="text-[10px] text-cq-muted">
            Szabályok: {d.meta.rules.map((r) => `${r.name}: ${r.description}`).join(' · ')}. Költségekkel nettó, bp.
          </div>
        </div>
      )}
      <PollNote poll={poll} what="scout" />
    </Card>
  )
}

