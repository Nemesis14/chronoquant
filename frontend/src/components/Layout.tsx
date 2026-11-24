import { NavLink, Outlet, useLocation, useMatch } from 'react-router-dom'
import { LineChart } from 'lucide-react'
import { getAssets } from '../api/dashboard'
import { loadLastAsset } from '../lib/asset'
import { usePolling } from '../lib/usePolling'
import AssetTabs from './AssetTabs'
import ChatDock from './chat/ChatDock'

const NAV = [
  { to: '/reports', label: 'Elemzések', end: false },
  { to: '/chat', label: 'Chat', end: false },
]

export default function Layout() {
  const match = useMatch('/a/:asset/*')
  // ChatDock is hidden on /chat -> no button to keep clear (and ChatPage sizes itself to 100vh)
  const dockVisible = !useLocation().pathname.startsWith('/chat')
  const current = match?.params.asset
  const assets = usePolling((s) => getAssets(s), 0)
  const items = (assets.data?.items ?? []).filter((a) => a.has_data)
  const dashTo = `/a/${current ?? loadLastAsset() ?? assets.data?.default_asset_id ?? 'solusdt'}`
  return (
    <div className="min-h-screen bg-cq-bg text-cq-text">
      <nav className="bg-cq-panel border-b border-cq-grid px-6 py-3 flex items-center gap-6">
        <span className="font-bold text-lg flex items-center gap-2">
          <LineChart size={20} className="text-cq-blue" />
          ChronoQuant
        </span>
        <NavLink
          to={dashTo}
          className={() =>
            `text-sm font-medium transition-colors ${current ? 'text-cq-blue' : 'text-cq-muted hover:text-cq-text'}`
          }
        >
          Dashboard
        </NavLink>
        {current && <AssetTabs items={items} current={current} />}
        {NAV.map((n) => (
          <NavLink
            key={n.to}
            to={n.to}
            end={n.end}
            className={({ isActive }) =>
              `text-sm font-medium transition-colors ${isActive ? 'text-cq-blue' : 'text-cq-muted hover:text-cq-text'}`
            }
          >
            {n.label}
          </NavLink>
        ))}
      </nav>
      <main className={`max-w-[1800px] mx-auto px-4 py-4${dockVisible ? ' cq-dock-clear' : ''}`}>
        <Outlet />
      </main>
      <ChatDock />
    </div>
  )
}
