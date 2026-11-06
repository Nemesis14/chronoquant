// Lazy Plotly wrapper (separate chunk). Usage:
//   const PlotlyFigure = lazy(() => import('./PlotlyFigure'))
//   <Suspense fallback=...><PlotlyFigure data={..} layout={..} /></Suspense>
import Plotly from 'plotly.js-dist-min'
import createPlotlyComponent from 'react-plotly.js/factory'
import type { Config, Data, Layout, PlotMouseEvent } from 'plotly.js'


const Plot = createPlotlyComponent(Plotly)

export const CQ_COLORS = {
  bg: '#0b0e11',
  panel: '#111418',
  grid: '#2b3139',
  text: '#eaecef',
  muted: '#848e9c',
  green: '#0ecb81',
  red: '#f6465d',
  gold: '#f0b90b',
  blue: '#5b8af5',
}

export const BASE_LAYOUT: Partial<Layout> = {
  paper_bgcolor: CQ_COLORS.bg,
  plot_bgcolor: CQ_COLORS.panel,
  font: { color: CQ_COLORS.text },
  margin: { l: 0, r: 60, t: 12, b: 40 },
  showlegend: false,
}

export const BASE_CONFIG: Partial<Config> = {
  scrollZoom: true,
  displaylogo: false,
  responsive: true,
}

interface Props {
  data: Data[]
  layout?: Partial<Layout>
  config?: Partial<Config>
  height?: number | string
  onClick?: (e: Readonly<PlotMouseEvent>) => void
}

export default function PlotlyFigure({ data, layout, config, height = 420, onClick }: Props) {
  return (
    <Plot
      data={data}
      layout={{ ...BASE_LAYOUT, autosize: true, ...layout }}
      config={{ ...BASE_CONFIG, ...config }}
      useResizeHandler
      style={{ width: '100%', height }}
      onClick={onClick}
    />
  )
}
