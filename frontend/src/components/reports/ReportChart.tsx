import { Suspense, lazy, useEffect, useMemo, useState } from 'react'
import type { Data, Layout } from 'plotly.js'
import { getReportChart } from '../../api/reports'
import type { ChartRef, ReportFigure } from '../../types/reports'
import { useInView } from './useInView'

// plotly.js-dist-min is a separate chunk, loaded when the first chart mounts.
const PlotlyFigure = lazy(() => import('../charts/PlotlyFigure'))

const TEXT = '#eaecef'
const GRID = '#2b3139'
const PANEL = '#111418'
// report palette ACCENT (#343a40) is invisible on the dark UI -> light grey
const DARK_ON_DARK = /#343a40/gi

function themedAxis(axis: unknown): Record<string, unknown> {
  const a = (axis ?? {}) as Record<string, unknown>
  return { gridcolor: GRID, zerolinecolor: GRID, linecolor: GRID, automargin: true, ...a }
}

/** Merge the dark UI theme into a template-less report figure. */
export function themeFigure(fig: ReportFigure): { data: Data[]; layout: Partial<Layout> } {
  const recolored = JSON.parse(JSON.stringify(fig).replace(DARK_ON_DARK, '#c9d1d9')) as ReportFigure
  const src = (recolored.layout ?? {}) as Record<string, unknown>
  const layout: Record<string, unknown> = {
    paper_bgcolor: 'rgba(0,0,0,0)',
    plot_bgcolor: PANEL,
    font: { color: TEXT, size: 12 },
    showlegend: fig.data.length > 1,
    margin: { l: 56, r: 24, t: 28, b: 48 },
    ...src,
    xaxis: themedAxis(src.xaxis),
    yaxis: themedAxis(src.yaxis),
  }
  for (const key of Object.keys(src)) {
    if (/^(x|y)axis\d+$/.test(key)) layout[key] = themedAxis(src[key])
  }
  const legend = (src.legend ?? {}) as Record<string, unknown>
  layout.legend = { bgcolor: 'rgba(0,0,0,0)', ...legend }
  return { data: recolored.data as unknown as Data[], layout: layout as Partial<Layout> }
}

interface Props {
  owner: string
  slug: string
  chart: ChartRef
}

/** Chart placeholder: fetches the figure only when it scrolls near the viewport. */
export default function ReportChart({ owner, slug, chart }: Props) {
  const { ref, seen } = useInView<HTMLElement>()
  const [fig, setFig] = useState<ReportFigure | null>(null)
  const [error, setError] = useState<string | null>(null)
  const height = chart.height ?? 420

  useEffect(() => {
    if (!seen) return
    const ctrl = new AbortController()
    getReportChart(owner, slug, chart.id, ctrl.signal)
      .then(setFig)
      .catch((e: unknown) => {
        if (e instanceof DOMException && e.name === 'AbortError') return
        setError(e instanceof Error ? e.message : 'Hiba a chart betöltésekor')
      })
    return () => ctrl.abort()
  }, [seen, owner, slug, chart.id])

  const themed = useMemo(() => (fig ? themeFigure(fig) : null), [fig])

  return (
    <figure ref={ref} className="my-4 rounded-md border border-cq-grid bg-cq-panel p-2">
      <figcaption className="px-1 pb-1 text-xs font-semibold text-cq-muted">{chart.title}</figcaption>
      <div style={{ minHeight: height }}>
        {error && <div className="p-3 text-xs text-cq-red">{error}</div>}
        {!error && !themed && <div className="p-3 text-xs text-cq-muted">Betöltés…</div>}
        {themed && (
          <Suspense fallback={<div className="p-3 text-xs text-cq-muted">Chart motor betöltése…</div>}>
            <PlotlyFigure data={themed.data} layout={themed.layout} height={height} />
          </Suspense>
        )}
      </div>
    </figure>
  )
}
