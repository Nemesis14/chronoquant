import { lazy, Suspense, useMemo } from 'react'
import type { Data } from 'plotly.js'
import type { EquityResponse } from '../../types/api'
import { COLORS } from '../../lib/format'

const PlotlyFigure = lazy(() => import('./PlotlyFigure'))

export default function EquityChart({ equity }: { equity: EquityResponse }) {
  const data = useMemo<Data[]>(() => {
    const series = equity.series ?? {}
    const mk = (name: 'long' | 'short', color: string): Data => ({
      type: 'scattergl', mode: 'lines', name,
      x: (series[name] ?? []).map((p) => p.t),
      y: (series[name] ?? []).map((p) => p.cum_fact_log_return),
      line: { color, width: 1.5, shape: 'hv' },
      hovertemplate: `${name} %{y:.4f}<extra></extra>`,
    })
    return [mk('long', COLORS.green), mk('short', COLORS.red)]
  }, [equity])

  return (
    <Suspense fallback={<div className="h-[360px] flex items-center justify-center text-xs text-cq-muted">Grafikon betöltése…</div>}>
      <PlotlyFigure
        data={data}
        height={360}
        layout={{
          showlegend: true,
          legend: { orientation: 'h', x: 0, y: 1.1, font: { color: COLORS.text } },
          hovermode: 'x unified',
          dragmode: 'pan',
          uirevision: 'cq-equity',
          xaxis: { gridcolor: COLORS.grid, tickfont: { color: COLORS.muted } },
          yaxis: { gridcolor: COLORS.grid, tickfont: { color: COLORS.muted }, side: 'right',
            title: { text: 'cum. log-return', font: { color: COLORS.muted, size: 11 } }, zeroline: true, zerolinecolor: COLORS.grid },
        }}
      />
    </Suspense>
  )
}
