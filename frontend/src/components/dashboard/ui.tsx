import type { ReactNode } from 'react'
import { ApiError } from '../../api/client'
import type { PollState } from '../../lib/usePolling'

export function Card({
  title,
  right,
  children,
  className = '',
}: {
  title?: ReactNode
  right?: ReactNode
  children: ReactNode
  className?: string
}) {
  return (
    <section className={`border border-cq-grid rounded-md bg-cq-panel p-3 ${className}`}>
      {(title || right) && (
        <header className="flex items-center justify-between mb-2">
          <h3 className="text-sm font-bold">{title}</h3>
          <div className="text-xs text-cq-muted flex items-center gap-2">{right}</div>
        </header>
      )}
      {children}
    </section>
  )
}

export function Muted({ children }: { children: ReactNode }) {
  return <div className="text-xs text-cq-muted">{children}</div>
}

/** Error / db_busy / loading line for a polled resource. Last data stays visible when present. */
export function PollNote<T>({ poll, what }: { poll: PollState<T>; what?: string }) {
  if (poll.loading) return <Muted>Betöltés…</Muted>
  if (!poll.error) return null
  if (poll.busy) {
    return (
      <div className="text-xs text-cq-gold">
        Az adatbázis foglalt (szinkron folyamatban) — újrapróbálás automatikusan
        {poll.data ? ', az utolsó adat látszik.' : '.'}
      </div>
    )
  }
  const e = poll.error
  const code = e instanceof ApiError ? ` [${e.code}]` : ''
  return (
    <div className="text-xs text-cq-red flex items-center gap-2">
      <span>
        {what ? `${what}: ` : ''}
        {e.message}
        {code}
      </span>
      <button className="underline text-cq-muted hover:text-cq-text" onClick={poll.reload}>
        újra
      </button>
    </div>
  )
}

export function Dot({ ok, warn }: { ok: boolean; warn?: boolean }) {
  const c = ok ? 'bg-cq-green' : warn ? 'bg-cq-gold' : 'bg-cq-red'
  return <span className={`inline-block w-2 h-2 rounded-full ${c}`} />
}

export function Spinner() {
  return <span className="inline-block w-3 h-3 border-2 border-cq-muted border-t-transparent rounded-full animate-spin" />
}

export function KV({ k, children }: { k: string; children: ReactNode }) {
  return (
    <>
      <span className="text-cq-muted">{k}</span>
      <span className="font-medium text-right">{children}</span>
    </>
  )
}

export interface Col<R> {
  key: string
  label: string
  render: (r: R) => ReactNode
  align?: 'right'
}

export function DataTable<R>({ cols, rows, maxH = 'max-h-80', empty = 'Nincs adat' }: {
  cols: Col<R>[]
  rows: R[]
  maxH?: string
  empty?: string
}) {
  if (!rows.length) return <Muted>{empty}</Muted>
  return (
    <div className={`overflow-auto ${maxH}`}>
      <table className="w-full text-xs">
        <thead className="sticky top-0 bg-cq-panel">
          <tr className="text-cq-muted border-b border-cq-grid">
            {cols.map((c) => (
              <th key={c.key} className={`py-1 px-2 font-normal ${c.align === 'right' ? 'text-right' : 'text-left'}`}>
                {c.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i} className="border-b border-cq-grid/50 hover:bg-white/5">
              {cols.map((c) => (
                <td key={c.key} className={`py-1 px-2 whitespace-nowrap ${c.align === 'right' ? 'text-right' : ''}`}>
                  {c.render(r)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
