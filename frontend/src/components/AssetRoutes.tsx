import { useEffect } from 'react'
import { Navigate, useParams } from 'react-router-dom'
import { getAssets } from '../api/dashboard'
import { AssetContext, loadLastAsset, saveLastAsset } from '../lib/asset'
import { usePolling } from '../lib/usePolling'
import DashboardPage from '../pages/DashboardPage'

/** `/` -> `/a/<last used asset or default>`. */
export function RootRedirect() {
  const poll = usePolling((s) => getAssets(s), 0)
  if (poll.loading) return <div className="p-4 text-sm text-cq-muted">Betöltés…</div>
  const list = poll.data
  const last = loadLastAsset()
  const target =
    list?.items.find((a) => a.asset_id === last)?.asset_id ?? list?.default_asset_id ?? last ?? 'solusdt'
  return <Navigate to={`/a/${target}`} replace />
}

/** `/a/:asset` -> dashboard; remounted per asset so no state/poll data of the other asset survives. */
export function AssetDashboard() {
  const { asset = '' } = useParams()
  const poll = usePolling((s) => getAssets(s), 0)
  useEffect(() => {
    saveLastAsset(asset)
  }, [asset])
  const list = poll.data
  if (list && !list.items.some((a) => a.asset_id === asset)) return <Navigate to="/" replace />
  return (
    <AssetContext.Provider value={asset}>
      <DashboardPage key={asset} />
    </AssetContext.Provider>
  )
}
