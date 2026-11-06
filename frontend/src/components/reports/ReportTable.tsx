import { useEffect, useMemo, useState } from 'react'
import { ArrowDown, ArrowUp } from 'lucide-react'
import { getReportTable } from '../../api/reports'
import type { ReportTableData, TableCell, TableColumn, TableRef } from '../../types/reports'
import { useInView } from './useInView'

export function formatCell(v: TableCell, col: TableColumn): string {
  if (v === null || v === undefined) return '—'
  switch (col.type) {
    case 'int':
      return typeof v === 'number' ? v.toLocaleString('en-US', { maximumFractionDigits: 0 }) : String(v)
    case 'float':
      return typeof v === 'number'
        ? v.toLocaleString('en-US', {
            minimumFractionDigits: col.digits ?? 4,
            maximumFractionDigits: col.digits ?? 4,
          })
        : String(v)
    case 'pct':
      return typeof v === 'number' ? `${(v * 100).toFixed(col.digits ?? 2)}%` : String(v)
    case 'bool':
      return v === true ? '✓' : v === false ? '✗' : String(v)
    case 'datetime': {
      const s = String(v).replace('T', ' ').replace('Z', '')
      return s.endsWith(' 00:00:00') ? s.slice(0, 10) : s.slice(0, 19)
    }
    default:
      return String(v)
  }
}

const numeric = (c: TableColumn) => c.type === 'int' || c.type === 'float' || c.type === 'pct'

interface Props {
  owner: string
  slug: string
  table: TableRef
}

/** Table placeholder: fetched lazily when it scrolls near the viewport; click a header to sort. */
export default function ReportTable({ owner, slug, table }: Props) {
  const { ref, seen } = useInView<HTMLDivElement>()
  const [data, setData] = useState<ReportTableData | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [sort, setSort] = useState<{ col: number; dir: 1 | -1 } | null>(null)

  useEffect(() => {
    if (!seen) return
    const ctrl = new AbortController()
    getReportTable(owner, slug, table.id, ctrl.signal)
      .then(setData)
      .catch((e: unknown) => {
        if (e instanceof DOMException && e.name === 'AbortError') return
        setError(e instanceof Error ? e.message : 'Hiba a tábla betöltésekor')
      })
    return () => ctrl.abort()
  }, [seen, owner, slug, table.id])

  const rows = useMemo(() => {
    if (!data) return []
    if (!sort) return data.rows
    const { col, dir } = sort
    return [...data.rows].sort((a, b) => {
      const x = a[col]
      const y = b[col]
      if (x === null) return 1
      if (y === null) return -1
      if (typeof x === 'number' && typeof y === 'number') return (x - y) * dir
      return String(x).localeCompare(String(y)) * dir
    })
  }, [data, sort])

  const toggleSort = (i: number) =>
    setSort((s) => (s?.col === i ? { col: i, dir: s.dir === 1 ? -1 : 1 } : { col: i, dir: 1 }))

  return (
    <div ref={ref} className="my-4">
      <div className="mb-1 text-xs font-semibold text-cq-muted">{table.title}</div>
      {error && <div className="text-xs text-cq-red">{error}</div>}
      {!error && !data && <div className="text-xs text-cq-muted">Betöltés…</div>}
      {data && (
        <div className="max-h-[440px] overflow-auto rounded-md border border-cq-grid">
          <table className="min-w-full border-collapse text-[12.5px]">
            <thead className="sticky top-0 bg-cq-panel">
              <tr>
                {data.columns.map((c, i) => (
                  <th
                    key={c.key}
                    onClick={() => toggleSort(i)}
                    className={`cursor-pointer select-none whitespace-nowrap border-b border-cq-grid px-2 py-1 font-semibold ${numeric(c) ? 'text-right' : 'text-left'}`}
                  >
                    {c.label}
                    {sort?.col === i &&
                      (sort.dir === 1 ? (
                        <ArrowUp size={11} className="ml-1 inline" />
                      ) : (
                        <ArrowDown size={11} className="ml-1 inline" />
                      ))}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((r, ri) => (
                <tr key={ri} className="odd:bg-black/10 hover:bg-cq-grid/30">
                  {r.map((v, ci) => (
                    <td
                      key={ci}
                      className={`whitespace-nowrap border-b border-cq-grid/50 px-2 py-0.5 ${numeric(data.columns[ci]) ? 'text-right tabular-nums' : ''}`}
                    >
                      {formatCell(v, data.columns[ci])}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {data?.truncated && (
        <div className="mt-1 text-xs text-cq-gold">
          Csonkolt: {data.rows.length.toLocaleString('en-US')} / {data.total_rows.toLocaleString('en-US')} sor.
        </div>
      )}
    </div>
  )
}
