import { NavLink, Outlet } from 'react-router-dom'
import { LineChart } from 'lucide-react'
import ChatDock from './chat/ChatDock'

const NAV = [
  { to: '/', label: 'Dashboard', end: true },
  { to: '/reports', label: 'Elemzések', end: false },
  { to: '/chat', label: 'Chat', end: false },
]

export default function Layout() {
  return (
    <div className="min-h-screen bg-cq-bg text-cq-text">
      <nav className="bg-cq-panel border-b border-cq-grid px-6 py-3 flex items-center gap-6">
        <span className="font-bold text-lg flex items-center gap-2">
          <LineChart size={20} className="text-cq-blue" />
          ChronoQuant
        </span>
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
      <main className="max-w-[1800px] mx-auto px-4 py-4">
        <Outlet />
      </main>
      <ChatDock />
    </div>
  )
}
