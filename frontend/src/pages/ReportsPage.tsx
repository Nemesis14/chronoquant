import { useMemo } from 'react'
import { useSearchParams } from 'react-router-dom'
import { FileText } from 'lucide-react'
import { getAssets } from '../api/dashboard'
import { getReport, listReports } from '../api/reports'
import { loadLastAsset } from '../lib/asset'
import { Card, Muted, PollNote } from '../components/dashboard/ui'
import ReportMarkdown from '../components/reports/ReportMarkdown'
import { usePolling } from '../lib/usePolling'
import type { ReportManifest } from '../types/reports'

const GENERAL = 'general'
const ALL = 'all'
/** asset filter value: all assets (model reports of every asset + general). */
const ANY_ASSET = 'all'

const ownerLabel = (owner: string) => (owner === GENERAL ? 'Általános' : owner)

const day = (iso: string | null | undefined) => (iso ? iso.replace('T', ' ').slice(0, 16) : '—')

function ReportListItem({
  report,
  active,
  onSelect,
}: {
  report: ReportManifest
  active: boolean
  onSelect: () => void
}) {
  return (
    <button
      type="button"
      onClick={onSelect}
      className={`w-full rounded-md border px-3 py-2 text-left transition-colors ${
        active ? 'border-cq-blue bg-cq-blue/10' : 'border-cq-grid hover:bg-cq-grid/30'
      }`}
    >
      <div className="flex items-center gap-2 text-sm font-semibold">
        <FileText size={14} className="shrink-0 text-cq-muted" />
        <span className="truncate">{report.title}</span>
      </div>
      {report.summary && <div className="mt-0.5 line-clamp-2 text-xs text-cq-muted">{report.summary}</div>}
      <div className="mt-1 text-[11px] text-cq-muted">
        {report.content.charts.length} ábra · {report.content.tables.length} tábla ·{' '}
        {day(report.updated_at ?? report.created_at)}
      </div>
    </button>
  )
}

function Reader({ owner, slug }: { owner: string; slug: string }) {
  const poll = usePolling((signal) => getReport(owner, slug, signal), 0, [owner, slug])
  const detail = poll.data
  // ignore a stale response of the previously selected report while the new one loads
  const current = detail && detail.manifest.id === slug && detail.manifest.owner === owner ? detail : null

  if (!current) {
    return (
      <div className="p-4">
        <PollNote poll={poll} what="Riport" />
      </div>
    )
  }
  const { manifest, markdown } = current
  return (
    <article className="px-5 py-4">
      <header className="mb-3 border-b border-cq-grid pb-3">
        <h1 className="text-xl font-bold">{manifest.title}</h1>
        <div className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-cq-muted">
          <span>{ownerLabel(manifest.owner)}</span>
          <span>Frissítve: {day(manifest.updated_at ?? manifest.created_at)} UTC</span>
          {manifest.tags.map((t) => (
            <span key={t} className="rounded bg-cq-grid/60 px-1.5 py-0.5">
              {t}
            </span>
          ))}
        </div>
      </header>
      <ReportMarkdown
        key={`${owner}/${slug}`}
        owner={owner}
        slug={slug}
        markdown={markdown}
        charts={manifest.content.charts}
        tables={manifest.content.tables}
      />
    </article>
  )
}

export default function ReportsPage() {
  const [params, setParams] = useSearchParams()
  const filter = params.get('filter') ?? ALL
  const selOwner = params.get('owner')
  const selSlug = params.get('slug')

  // Manifest list only; chart / table / markdown content is fetched on click / scroll.
  const poll = usePolling((signal) => listReports(undefined, signal), 0, [])
  const assetsPoll = usePolling((signal) => getAssets(signal), 0, [])
  const assetItems = useMemo(() => assetsPoll.data?.items ?? [], [assetsPoll.data])
  // asset filter lives in ?asset=; default = last used dashboard asset
  const assetParam = params.get('asset')
  const assetFilter = assetParam ?? loadLastAsset() ?? ANY_ASSET
  // model reports of the chosen asset + general reports (not asset-specific)
  const all = useMemo(
    () =>
      (poll.data?.items ?? []).filter(
        (r) => assetFilter === ANY_ASSET || !r.asset_id || r.asset_id === assetFilter,
      ),
    [poll.data, assetFilter],
  )

  const owners = useMemo(() => {
    const set = new Set(all.map((r) => r.owner))
    return [...set].sort((a, b) => (a === GENERAL ? -1 : b === GENERAL ? 1 : a.localeCompare(b)))
  }, [all])

  const visible = useMemo(
    () => (filter === ALL ? all : all.filter((r) => r.owner === filter)),
    [all, filter],
  )

  const select = (r: ReportManifest) => {
    const next = new URLSearchParams(params)
    next.set('owner', r.owner)
    next.set('slug', r.id)
    setParams(next, { replace: true })
  }

  const setFilter = (value: string) => {
    const next = new URLSearchParams(params)
    if (value === ALL) next.delete('filter')
    else next.set('filter', value)
    setParams(next, { replace: true })
  }

  const setAssetFilter = (value: string) => {
    const next = new URLSearchParams(params)
    next.set('asset', value)
    next.delete('filter')
    setParams(next, { replace: true })
  }

  return (
    <div className="grid grid-cols-1 gap-3 lg:grid-cols-[320px_1fr]">
      <Card title="Elemzések" className="lg:h-[calc(100vh-96px)] lg:overflow-y-auto">
        <label className="mb-2 block text-xs text-cq-muted">
          Asset
          <select
            value={assetFilter}
            onChange={(e) => setAssetFilter(e.target.value)}
            className="mt-1 block w-full rounded border border-cq-grid bg-cq-bg px-2 py-1 text-sm text-cq-text"
          >
            <option value={ANY_ASSET}>Mind</option>
            {assetItems.map((a) => (
              <option key={a.asset_id} value={a.asset_id}>
                {a.symbol}
              </option>
            ))}
          </select>
        </label>
        <label className="mb-2 block text-xs text-cq-muted">
          Szűrő (modell / scope)
          <select
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            className="mt-1 block w-full rounded border border-cq-grid bg-cq-bg px-2 py-1 text-sm text-cq-text"
          >
            <option value={ALL}>Mind ({all.length})</option>
            {owners.map((o) => (
              <option key={o} value={o}>
                {ownerLabel(o)} ({all.filter((r) => r.owner === o).length})
              </option>
            ))}
          </select>
        </label>
        <PollNote poll={poll} what="Riport lista" />
        {!poll.loading && !poll.error && visible.length === 0 && <Muted>Nincs riport.</Muted>}
        <div className="space-y-3">
          {owners
            .filter((o) => visible.some((r) => r.owner === o))
            .map((o) => (
              <div key={o}>
                <div className="mb-1 break-all text-[11px] font-semibold uppercase tracking-wide text-cq-muted">
                  {ownerLabel(o)}
                </div>
                <div className="space-y-1.5">
                  {visible
                    .filter((r) => r.owner === o)
                    .map((r) => (
                      <ReportListItem
                        key={`${r.owner}/${r.id}`}
                        report={r}
                        active={r.owner === selOwner && r.id === selSlug}
                        onSelect={() => select(r)}
                      />
                    ))}
                </div>
              </div>
            ))}
        </div>
      </Card>

      <section className="min-w-0 rounded-md border border-cq-grid bg-cq-panel lg:h-[calc(100vh-96px)] lg:overflow-y-auto">
        {selOwner && selSlug ? (
          <Reader owner={selOwner} slug={selSlug} />
        ) : (
          <div className="p-6 text-sm text-cq-muted">Válassz egy riportot a bal oldali listából.</div>
        )}
      </section>
    </div>
  )
}
