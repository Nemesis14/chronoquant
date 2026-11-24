import { getActiveStrategies, getActiveTrade, getBinanceFills, getRegistryActive } from '../../api/dashboard'
import { ApiError } from '../../api/client'
import { useAsset } from '../../lib/asset'
import { usePolling } from '../../lib/usePolling'
import { ago, DASH, num, pct, pnlColor, shortTime, sideColor, signed } from '../../lib/format'
import type { Status, StrategyCard } from '../../types/api'
import { Card, DataTable, KV, Muted, PollNote, Spinner } from './ui'

/** Active position. Slow (Binance fallback ~4 s) -> own poll, never blocks the chart. */
export function ActiveTradeCard() {
  const asset = useAsset()
  const poll = usePolling((s) => getActiveTrade(asset, s), 30_000, [asset])
  const notFound = poll.error instanceof ApiError && poll.error.status === 404
  const p = notFound ? null : poll.data
  return (
    <Card title="Aktív trade" right={poll.refreshing && <Spinner />}>
      {p ? (
        <div className="grid grid-cols-[110px_1fr] gap-y-1 text-xs">
          <KV k="Irány"><span className={sideColor(p.side)}>{(p.side ?? DASH).toUpperCase()}</span></KV>
          <KV k="Entry">{num(p.entry_price, 4)}</KV>
          {p.stop_loss !== null && <KV k="Stop loss"><span className="text-cq-red">{num(p.stop_loss, 4)}</span></KV>}
          {p.take_profit !== null && <KV k="Take profit"><span className="text-cq-green">{num(p.take_profit, 4)}</span></KV>}
          <KV k="Mennyiség">{num(p.quantity, 2)}</KV>
          <KV k="Nyitva">{shortTime(p.entry_time)}</KV>
          <KV k="Forrás">{p.source}</KV>
        </div>
      ) : (
        !poll.loading && !poll.error && <Muted>Nincs aktív trade</Muted>
      )}
      {notFound && <Muted>Nincs aktív trade</Muted>}
      {!notFound && <PollNote poll={poll} what="aktív trade" />}
    </Card>
  )
}

function StratRow({ side, c }: { side: 'long' | 'short'; c: StrategyCard | null }) {
  const color = side === 'long' ? 'text-cq-green' : 'text-cq-red'
  if (!c) return <Muted>{side}: nincs adat</Muted>
  const m = c.metrics
  return (
    <div className="border-b border-cq-grid/60 last:border-0 py-2 text-xs">
      <div className={`font-bold ${color} mb-1`}>
        {side === 'long' ? '▲ LONG' : '▼ SHORT'}{' '}
        <span className="text-cq-muted font-normal">entry {pct(c.entry_cutoff, 0)}</span>
      </div>
      <div className="grid grid-cols-[110px_1fr] gap-y-0.5">
        <KV k="TP / SL">{c.tp_spec ?? DASH} / {c.sl_spec ?? DASH}</KV>
        <KV k="Max hold">{c.max_hold_minutes ?? DASH} min</KV>
        <KV k="Trades">{m.n_trades ?? DASH}</KV>
        <KV k="Win rate">{pct(m.win_rate)}</KV>
        <KV k="Total log-ret"><span className={pnlColor(m.total_fact_log_return)}>{signed(m.total_fact_log_return)}</span></KV>
        <KV k="Compounded"><span className={pnlColor(m.compounded_return_pct)}>{m.compounded_return_pct === null ? DASH : `${signed(m.compounded_return_pct, 1)}%`}</span></KV>
        <KV k="Átl. hold">{m.avg_hold_minutes === null ? DASH : `${m.avg_hold_minutes.toFixed(1)} min`}</KV>
      </div>
      <div className="text-[11px] text-cq-muted mt-1 break-all">
        {c.session_id} · {c.fit_period?.start?.slice(0, 7) ?? DASH} → {c.fit_period?.end?.slice(0, 7) ?? DASH}
      </div>
    </div>
  )
}

export function StrategyPanel() {
  const asset = useAsset()
  const poll = usePolling((s) => getActiveStrategies(asset, s), 120_000, [asset])
  return (
    <Card title="Aktív stratégia" right={poll.refreshing && <Spinner />}>
      <StratRow side="long" c={poll.data?.long ?? null} />
      <StratRow side="short" c={poll.data?.short ?? null} />
      <PollNote poll={poll} what="stratégia" />
    </Card>
  )
}

export function ModelsPanel({ status }: { status: Status | null }) {
  const asset = useAsset()
  const reg = usePolling((s) => getRegistryActive(asset, s), 120_000, [asset])
  const live = status?.live
  return (
    <Card title="Modellek és adat" right={reg.refreshing && <Spinner />}>
      <div className="grid grid-cols-[110px_1fr] gap-y-1 text-xs">
        <KV k="Long modell"><span className="break-all">{status?.models.long.model_id ?? reg.data?.live_models.long ?? DASH}</span></KV>
        <KV k="Short modell"><span className="break-all">{status?.models.short.model_id ?? reg.data?.live_models.short ?? DASH}</span></KV>
        <KV k="Long strat"><span className="break-all">{reg.data?.strategy_sessions.long ?? DASH}</span></KV>
        <KV k="Short strat"><span className="break-all">{reg.data?.strategy_sessions.short ?? DASH}</span></KV>
        {live && (
          <>
            <KV k="OHLCV">{live.ohlcv.rows.toLocaleString('en-US')} · {ago(live.ohlcv.latest_open_time)}</KV>
            <KV k="Features">{live.features.rows.toLocaleString('en-US')} · {ago(live.features.latest_open_time)}</KV>
            <KV k="Predikciók">{live.predictions.rows.toLocaleString('en-US')} · {ago(live.predictions.latest_open_time)}</KV>
          </>
        )}
      </div>
      {(reg.data?.pending_deployments.length ?? 0) > 0 && (
        <Muted>Függő deploy: {reg.data?.pending_deployments.length}</Muted>
      )}
      <PollNote poll={reg} what="registry" />
    </Card>
  )
}

export function BinanceFillsPanel() {
  const asset = useAsset()
  const poll = usePolling((s) => getBinanceFills(asset, 60, s), 120_000, [asset])
  const unavailable = poll.error instanceof ApiError && poll.error.code === 'binance_unavailable'
  const items = poll.data?.items ?? []
  return (
    <Card
      title={`Utolsó trade-ek (Binance${poll.data ? ` · ${poll.data.source}` : ''})`}
      right={poll.refreshing && <Spinner />}
    >
      {unavailable ? (
        <Muted>Binance nem elérhető (kulcs / hálózat).</Muted>
      ) : (
        <>
          <DataTable
            maxH="max-h-72"
            rows={items.slice(0, 20)}
            cols={[
              { key: 'side', label: 'Irány', render: (r) => <span className={`font-semibold ${sideColor(r.side)}`}>{r.side}</span> },
              { key: 'price', label: 'Ár', render: (r) => num(r.price, 4) },
              { key: 'qty', label: 'Menny.', render: (r) => num(r.qty, 2) },
              { key: 'pnl', label: 'PnL', render: (r) => <span className={pnlColor(r.realized_pnl)}>{signed(r.realized_pnl)}</span> },
              { key: 't', label: 'Idő', render: (r) => shortTime(r.t) },
            ]}
          />
          <PollNote poll={poll} what="binance" />
        </>
      )}
    </Card>
  )
}
