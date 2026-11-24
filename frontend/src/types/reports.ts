// Report format (blueprint §3.3). Unknown manifest fields are tolerated by the backend.

export interface ChartRef {
  id: string
  title: string
  file: string
  height?: number
}

export interface TableRef {
  id: string
  title: string
  file: string
}

export interface ReportManifest {
  schema_version: number
  id: string
  /** "general" or the model_id (added by the API) */
  owner: string
  title: string
  scope: 'model' | 'general'
  model_id: string | null
  /** asset of the owning model (added by the API); null for general reports */
  asset_id?: string | null
  created_at: string
  updated_at: string | null
  order: number
  summary: string | null
  tags: string[]
  source?: Record<string, unknown> | null
  content: { markdown: string; charts: ChartRef[]; tables: TableRef[] }
}

export interface ReportList {
  items: ReportManifest[]
  total: number
}

export interface ReportDetail {
  manifest: ReportManifest
  markdown: string
}

/** Plotly figure JSON written by the report builder (no template, plain arrays). */
export interface ReportFigure {
  data: Array<Record<string, unknown>>
  layout?: Record<string, unknown>
  config?: Record<string, unknown>
}

export type ColumnType = 'int' | 'float' | 'pct' | 'str' | 'datetime' | 'bool'

export interface TableColumn {
  key: string
  label: string
  type: ColumnType
  digits?: number
}

export type TableCell = string | number | boolean | null

export interface ReportTableData {
  columns: TableColumn[]
  rows: TableCell[][]
  total_rows: number
  truncated: boolean
}
