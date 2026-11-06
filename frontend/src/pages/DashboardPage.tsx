import { useState } from 'react'
import { getChart, getStatus } from '../api/dashboard'
import PredictionPriceChart from '../components/charts/PredictionPriceChart'
import SyncPanel from '../components/dashboard/SyncPanel'
import { BinanceFillsPanel, ActiveTradeCard, ModelsPanel, StrategyPanel } from '../components/dashboard/SidePanels'
import TradeTabs from '../components/dashboard/TradeTabs'
import { Card, Dot, Muted, PollNote, Spinner } from '../components/dashboard/ui'
import { ago, DASH } from '../lib/format'
import { usePolling } from '../lib/usePolling'

const WINDOWS = ['24h', '8h', '4h']

export default function DashboardPage() {
  const [win, setWin] = useState('24h')
  const status = usePolling((s) => getStatus(s), 4000)
  // NOTE: /api/market/chart can take ~6 s when it falls back to Binance for the active trade;
  // data stays visible (stale) while it refreshes.
  const chart = usePolling((s) => getChart(win, s), 60_000, [win])
  const st = status.data
  const syncing = st?.sync.running ?? false

  return (
    <div className="space-y-3">
      <div className="flex items-center gap-4 flex-wrap border-b border-cq-grid pb-2">
        <span className="text-xl font-bold tracking-wide">{st?.symbol ?? 'ChronoQuant'}</span>
        <span className="text-cq-muted text-sm">{st?.interval ?? ''}</span>
        <span className="flex items-center gap-1 text-xs text-cq-muted">
          <Dot ok={st?.db.state === 'ok'} warn={st?.db.state === 'busy'} /> DB {st?.db.state ?? DASH}
        </span>
        {st?.live && (
          <span className="text-xs text-cq-muted">
            utolsó gyertya: {ago(st.live.ohlcv.latest_open_time)} ezelőtt
          </span>
        )}
        {syncing && <span className="text-xs text-cq-blue flex items-center gap-1"><Spinner /> szinkron fut</span>}
        <div className="ml-auto flex gap-1">
          {WINDOWS.map((w) => (
            <button
              key={w}
              onClick={() => setWin(w)}
              className={`px-3 py-1 text-xs rounded border ${win === w ? 'border-cq-blue text-cq-blue' : 'border-cq-grid text-cq-muted hover:text-cq-text'}`}
            >
              {w}
            </button>
          ))}
        </div>
      </div>
      <PollNote poll={status} what="status" />

      <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,3fr)_minmax(320px,1fr)] gap-3">
        <div className="space-y-3 min-w-0">
          <Card
            right={
              <>
                {chart.refreshing && <Spinner />}
                {chart.refreshing && chart.data && <span>frissítés…</span>}
              </>
            }
          >
            {chart.data ? (
              <PredictionPriceChart chart={chart.data} />
            ) : (
              <div className="h-[700px] flex items-center justify-center">
                {chart.loading ? <Muted>Grafikon betöltése… (az első hívás Binance fallback miatt lassú lehet)</Muted> : null}
              </div>
            )}
            <PollNote poll={chart} what="grafikon" />
          </Card>
          <TradeTabs />
        </div>
        <div className="space-y-3 min-w-0">
          <SyncPanel status={st} onChanged={status.reload} />
          <ActiveTradeCard />
          <StrategyPanel />
          <ModelsPanel status={st} />
          <BinanceFillsPanel />
        </div>
      </div>
    </div>
  )
}

