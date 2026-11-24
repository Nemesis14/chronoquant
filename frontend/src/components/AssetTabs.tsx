import { NavLink } from 'react-router-dom'
import type { AssetItem } from '../types/api'

/** Asset tab bar; order = `/api/assets` (config/assets.json `display_order`: BTC, BCH, SOL, ETH). Tabs link to the dashboard of that asset. */
export default function AssetTabs({ items, current }: { items: AssetItem[]; current: string }) {
  const tabs = items.some((a) => a.asset_id === current)
    ? items
    : [...items, { asset_id: current, symbol: current.toUpperCase() } as AssetItem]
  return (
    <div className="flex gap-1">
      {tabs.map((a) => (
        <NavLink
          key={a.asset_id}
          to={`/a/${a.asset_id}`}
          title={`${a.symbol}${a.active ? '' : ' (nincs auto-sync)'}`}
          className={() =>
            `px-3 py-1 text-xs rounded border font-semibold ${
              a.asset_id === current
                ? 'border-cq-blue text-cq-blue bg-cq-blue/10'
                : 'border-cq-grid text-cq-muted hover:text-cq-text'
            }`
          }
        >
          {a.symbol.replace(/USDT$/, '')}
        </NavLink>
      ))}
    </div>
  )
}
