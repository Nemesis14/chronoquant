import { useState } from 'react'
import { getClosedTrades, getEquity, getJournal } from '../../api/dashboard'
import { useAsset } from '../../lib/asset'
import { usePolling } from '../../lib/usePolling'
import { DASH, num, pct, pnlColor, shortTime, sideColor, signed } from '../../lib/format'
import type { Row } from '../../types/api'
import EquityChart from '../charts/EquityChart'
import LogPanel from './LogPanel'
import { Col, DataTable, Muted, PollNote, Spinner } from './ui'

function EquityTab() {
  const asset = useAsset()
  const poll = usePolling((s) => getEquity(asset, s), 300_000, [asset])
  const sm = poll.data?.summary
  return (
    <div>
      {sm && (
        <div className="flex flex-wrap gap-x-6 gap-y-1 text-xs mb-2">
          {(['long', 'short'] as const).map((d) => (
            <span key={d}>
              <b className={d === 'long' ? 'text-cq-green' : 'text-cq-red'}>{d.toUpperCase()}</b>{' '}
              <span className="text-cq-muted">trades</span> {sm[d]?.n_trades ?? DASH} ·{' '}
              <span className="text-cq-muted">win</span> {pct(sm[d]?.win_rate)} ·{' '}
              <span className="text-cq-muted">log-ret</span> {signed(sm[d]?.total_fact_log_return)} ·{' '}
              <span className="text-cq-muted">comp.</span>{' '}
              {sm[d]?.compounded_return_pct == null ? DASH : `${signed(sm[d].compounded_return_pct, 1)}%`}
            </span>
          ))}
        </div>
      )}
      {poll.data ? <EquityChart equity={poll.data} /> : poll.loading && <Muted>Betöltés…</Muted>}
      <PollNote poll={poll} what="equity" />
    </div>
  )
}

function ClosedTab() {
  const asset = useAsset()
  const poll = usePolling((s) => getClosedTrades(asset, 300, s), 300_000, [asset])
  const items = poll.data?.items ?? []
  return (
    <div>
      <Muted>Backtest zárt trade-ek (strat táblák) · összesen {poll.data?.total ?? DASH}</Muted>
      <DataTable
        maxH="max-h-96"
        rows={items}
        cols={[
          { key: 'side', label: 'Irány', render: (r) => <span className={`font-semibold ${sideColor(r.side)}`}>{(r.side ?? '').toUpperCase()}</span> },
          { key: 'in', label: 'Belépés', render: (r) => shortTime(r.entry_time) },
          { key: 'out', label: 'Kilépés', render: (r) => shortTime(r.exit_time) },
          { key: 'pin', label: 'Ár be', align: 'right', render: (r) => num(r.entry_price, 3) },
          { key: 'pout', label: 'Ár ki', align: 'right', render: (r) => num(r.exit_price, 3) },
          { key: 'lr', label: 'Log-ret', align: 'right', render: (r) => <span className={pnlColor(r.fact_log_return)}>{signed(r.fact_log_return, 5)}</span> },
          { key: 'why', label: 'Kilépés oka', render: (r) => r.exit_reason ?? DASH },
        ]}
      />
      <PollNote poll={poll} what="trade-ek" />
    </div>
  )
}

type JKind = 'positions' | 'signals' | 'orders' | 'errors' | 'runs'
const JOURNAL_COLS: Record<JKind, string[]> = {
  positions: ['side', 'status', 'entry_time', 'exit_time', 'entry_price', 'exit_price', 'quantity', 'pnl_usdt', 'exit_reason'],
  signals: ['bar_open_time', 'decision', 'reason', 'state'],
  orders: ['created_at', 'side', 'order_type', 'status', 'quantity', 'price'],
  errors: ['error_time', 'component', 'error_type', 'message'],
  runs: ['run_id', 'started_at', 'stopped_at', 'mode', 'asset_id'],
}

function cell(v: Row[string], key: string) {
  if (v === null || v === undefined) return DASH
  if (key === 'side') return <span className={sideColor(String(v))}>{String(v)}</span>
  if (key.endsWith('_time') || key.endsWith('_at')) return shortTime(String(v))
  if (typeof v === 'number') return key === 'pnl_usdt' ? <span className={pnlColor(v)}>{signed(v, 2)}</span> : num(v, 4)
  return String(v)
}

function JournalTab() {
  const [kind, setKind] = useState<JKind>('positions')
  const asset = useAsset()
  const poll = usePolling((s) => getJournal(asset, kind, 100, s), 60_000, [asset, kind])
  const rows = poll.data?.items ?? []
  const keys = (rows[0] ? Object.keys(rows[0]) : JOURNAL_COLS[kind]).filter((k) => !k.endsWith('_json'))
  const cols: Col<Row>[] = keys.slice(0, 12).map((k) => ({ key: k, label: k, render: (r) => cell(r[k], k) }))
  return (
    <div>
      <div className="flex gap-2 mb-2 text-xs items-center">
        {(Object.keys(JOURNAL_COLS) as JKind[]).map((k) => (
          <button
            key={k}
            onClick={() => setKind(k)}
            className={`px-2 py-0.5 rounded border ${kind === k ? 'border-cq-blue text-cq-blue' : 'border-cq-grid text-cq-muted'}`}
          >
            {k}
          </button>
        ))}
        {poll.refreshing && <Spinner />}
        <span className="text-cq-muted ml-auto">Csak olvasható · összesen {poll.data?.total ?? DASH}</span>
      </div>
      <DataTable cols={cols} rows={rows} maxH="max-h-96" empty="A journal üres" />
      <PollNote poll={poll} what="journal" />
    </div>
  )
}

const TABS = [
  { id: 'equity', label: 'Equity' },
  { id: 'closed', label: 'Zárt trade-ek' },
  { id: 'journal', label: 'Trade journal' },
  { id: 'log', label: 'Log' },
] as const

export default function TradeTabs() {
  const [tab, setTab] = useState<(typeof TABS)[number]['id']>('equity')
  return (
    <section className="border border-cq-grid rounded-md bg-cq-panel p-3">
      <div className="flex gap-4 border-b border-cq-grid mb-3">
        {TABS.map((t) => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={`pb-2 text-sm font-medium border-b-2 -mb-px ${tab === t.id ? 'border-cq-blue text-cq-blue' : 'border-transparent text-cq-muted hover:text-cq-text'}`}
          >
            {t.label}
          </button>
        ))}
      </div>
      {tab === 'equity' && <EquityTab />}
      {tab === 'closed' && <ClosedTab />}
      {tab === 'journal' && <JournalTab />}
      {tab === 'log' && <LogPanel />}
    </section>
  )
}

