import api from './client'
import type { ReportDetail, ReportFigure, ReportList, ReportTableData } from '../types/reports'

const base = (owner: string, slug: string) =>
  `reports/${encodeURIComponent(owner)}/${encodeURIComponent(slug)}`

/** Manifest list only (no content). */
export const listReports = (
  query?: { scope?: 'model' | 'general'; model_id?: string },
  signal?: AbortSignal,
) => api.get<ReportList>('reports', query, signal)

/** Manifest + markdown of one report (fetched on click). */
export const getReport = (owner: string, slug: string, signal?: AbortSignal) =>
  api.get<ReportDetail>(base(owner, slug), undefined, signal)

/** Plotly figure JSON (fetched lazily when the chart scrolls into view). */
export const getReportChart = (owner: string, slug: string, id: string, signal?: AbortSignal) =>
  api.get<ReportFigure>(`${base(owner, slug)}/charts/${encodeURIComponent(id)}`, undefined, signal)

export const getReportTable = (owner: string, slug: string, id: string, signal?: AbortSignal) =>
  api.get<ReportTableData>(`${base(owner, slug)}/tables/${encodeURIComponent(id)}`, undefined, signal)
