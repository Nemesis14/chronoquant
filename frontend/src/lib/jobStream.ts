import { useEffect, useRef, useState } from 'react'
import type { JobResult } from '../types/api'

export interface JobStream {
  lines: string[]
  status: string | null
  done: { status: string; result: JobResult | null; error: string | null } | null
  /** SSE connection problem (the log keeps what it already has) */
  connError: boolean
}

const MAX_LINES = 1000
const EMPTY: JobStream = { lines: [], status: null, done: null, connError: false }

/** Subscribes to GET /api/jobs/{id}/events (SSE: status / log / done). Reconnects via ?from=<offset>. */
export function useJobStream(jobId: string | null): JobStream {
  const [state, setState] = useState<JobStream>(EMPTY)
  const offsetRef = useRef(0)

  useEffect(() => {
    offsetRef.current = 0
    setState(EMPTY)
    if (!jobId) return
    let es: EventSource | null = null
    let closed = false
    let retry: ReturnType<typeof setTimeout> | undefined

    const connect = () => {
      if (closed) return
      es = new EventSource(`/api/jobs/${encodeURIComponent(jobId)}/events?from=${offsetRef.current}`)
      es.addEventListener('status', (ev) => {
        const d = JSON.parse((ev as MessageEvent).data) as { status: string }
        setState((s) => ({ ...s, status: d.status, connError: false }))
      })
      es.addEventListener('log', (ev) => {
        const d = JSON.parse((ev as MessageEvent).data) as { offset: number; line: string }
        offsetRef.current = d.offset
        setState((s) => ({ ...s, lines: [...s.lines, d.line].slice(-MAX_LINES), connError: false }))
      })
      es.addEventListener('done', (ev) => {
        const d = JSON.parse((ev as MessageEvent).data) as {
          status: string
          result: JobResult | null
          error: string | null
        }
        closed = true
        es?.close()
        setState((s) => ({ ...s, status: d.status, done: d }))
      })
      es.onerror = () => {
        es?.close()
        if (closed) return
        setState((s) => ({ ...s, connError: true }))
        retry = setTimeout(connect, 3000)
      }
    }
    connect()
    return () => {
      closed = true
      es?.close()
      if (retry) clearTimeout(retry)
    }
  }, [jobId])

  return state
}
