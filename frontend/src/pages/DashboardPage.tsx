import { useEffect, useState } from 'react'
import { getChart, getStatus } from '../api/dashboard'
import { getScoutLatest } from '../api/scout'
import ScoutEventChart from '../components/charts/ScoutEventChart'
import ScoutPanel, { ExperimentalBadge } from '../components/dashboard/ScoutPanel'
import PredictionPriceChart from '../components/charts/PredictionPriceChart'
import SyncPanel from '../components/dashboard/SyncPanel'
import { BinanceFillsPanel, ActiveTradeCard, ModelsPanel, StrategyPanel } from '../components/dashboard/SidePanels'
import TradeTabs from '../components/dashboard/TradeTabs'
import { Card, Dot, Muted, PollNote, Spinner } from '../components/dashboard/ui'
import { ago, DASH, shortTime } from '../lib/format'
import { eventStateText, SCOUT_COLOR } from '../lib/scout'
import { useAsset } from '../lib/asset'
import { usePolling } from '../lib/usePolling'

const WINDOWS = ['24h', '8h', '4h']
const SCOUT_LAYER_KEY = 'cq_scout_layer'

function loadScoutLayer(): boolean {
  try {
    return localStorage.getItem(SCOUT_LAYER_KEY) !== '0'
  } catch {
    return true
  }
}

export default function DashboardPage() {
  const asset = useAsset()
  const [win, setWin] = useState('24h')
  const status = usePolling((s) => getStatus(asset, s), 4000, [asset])
  // NOTE: /api/market/chart can take ~6 s when it falls back to Binance for the active trade;
  // data stays visible (stale) while it refreshes.
  const chart = usePolling((s) => getChart(asset, win, s), 60_000, [asset, win])
  // Impulse Scout (t943): one poll feeds the chart layer, the panel and the focus chart
  const scout = usePolling((s) => getScoutLatest(asset, 20, s), 60_000, [asset])
  const [scoutLayer, setScoutLayer] = useState(loadScoutLayer)
  const [scoutSel, setScoutSel] = useState<string | null>(null)
  useEffect(() => setScoutSel(null), [asset])
  const toggleScoutLayer = () => {
    const v = !scoutLayer
    setScoutLayer(v)
    try {
      localStorage.setItem(SCOUT_LAYER_KEY, v ? '1' : '0')
    } catch {
      /* storage unavailable */
    }
  }
  const scoutOk = scout.data?.available ?? false
  const scoutEvents = scoutOk && scout.data
    ? [...scout.data.open_events, ...scout.data.recent.filter((e) => e.status !== 'open')]
    : []
  const scoutSelected = scoutEvents.find((e) => e.event_id === scoutSel) ?? null
  const st = status.data
  const syncing = st?.sync.running ?? false
  // data-only asset (e.g. ETH, t946): no live quant model -> strategy / active-trade panels hidden
  // (st.live === null = DB busy -> model ids unknown, not "no model": keep the quant view)
  const noQuant = !!st && st.live !== null && !st.models.long.model_id && !st.models.short.model_id

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
      {st && noQuant && (
        <div className="text-xs text-cq-muted border border-cq-grid rounded px-3 py-2">
          Nincs quant modell ehhez az asset-hez ({st.symbol}) — csak adat: gyertyák, szinkron és az Impulse Scout.
          A predikció- és stratégia-panelek rejtve vannak.
        </div>
      )}

      <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,3fr)_minmax(320px,1fr)] gap-3">
        <div className="space-y-3 min-w-0">
          <Card
            right={
              <>
                {scoutOk && (
                  <button
                    onClick={toggleScoutLayer}
                    aria-pressed={scoutLayer}
                    title={`Impulse Scout réteg a charton: ${scoutLayer ? 'be' : 'ki'} (kísérleti – nem kereskedési jel)`}
                    className={`flex items-center gap-1 px-2 py-0.5 rounded border text-xs ${scoutLayer ? 'text-cq-text' : 'border-cq-grid text-cq-muted hover:text-cq-text'}`}
                    style={scoutLayer ? { borderColor: SCOUT_COLOR } : undefined}
                  >
                    <span className="inline-block w-2 h-2 rotate-45" style={{ background: scoutLayer ? SCOUT_COLOR : 'transparent', border: `1px solid ${SCOUT_COLOR}` }} />
                    Scout réteg
                  </button>
                )}
                {chart.refreshing && <Spinner />}
                {chart.refreshing && chart.data && <span>frissítés…</span>}
              </>
            }
          >
            {chart.data && chart.data.bars.length > 0 ? (
              <PredictionPriceChart chart={chart.data} scout={scoutLayer && scoutOk ? scoutEvents : null} selectedId={scoutSel} quant={!noQuant} />
            ) : chart.data ? (
              <div className="h-[480px] flex items-center justify-center">
                <Muted>Nincs megjeleníthető adat ehhez az asset-hez (még nincs szinkronizált gyertya / predikció).</Muted>
              </div>
            ) : (
              <div className="h-[480px] flex items-center justify-center">
                {chart.loading ? <Muted>Grafikon betöltése… (az első hívás Binance fallback miatt lassú lehet)</Muted> : null}
              </div>
            )}
            <PollNote poll={chart} what="grafikon" />
          </Card>
          {scoutSelected && (
            <Card
              title={
                <span className="flex items-center gap-2 flex-wrap">
                  <span className="inline-block w-2.5 h-2.5 rotate-45" style={{ background: SCOUT_COLOR }} />
                  Scout esemény · {st?.symbol ?? asset.toUpperCase()} · t_c {shortTime(scoutSelected.t_c)} · {eventStateText(scoutSelected)}
                  <ExperimentalBadge compact />
                </span>
              }
              right={<button className="underline hover:text-cq-text" onClick={() => setScoutSel(null)}>bezárás</button>}
            >
              <ScoutEventChart asset={asset} event={scoutSelected} />
            </Card>
          )}
          <TradeTabs />
        </div>
        <div className="space-y-3 min-w-0">
          <SyncPanel status={st} onChanged={status.reload} />
          <ScoutPanel poll={scout} selectedId={scoutSel} onSelect={setScoutSel} />
          {!noQuant && <ActiveTradeCard />}
          {!noQuant && <StrategyPanel />}
          <ModelsPanel status={st} />
          <BinanceFillsPanel />
        </div>
      </div>
    </div>
  )
}

