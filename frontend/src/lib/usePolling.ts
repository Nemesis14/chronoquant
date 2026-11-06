import { useCallback, useEffect, useRef, useState } from 'react'
import { ApiError } from '../api/client'

export interface PollState<T> {
  data: T | null
  /** true only until the first response (success or failure) */
  loading: boolean
  /** a request is in flight (data may be stale) */
  refreshing: boolean
  error: ApiError | Error | null
  /** last error was a 503 db_busy: data is kept, UI shows "sync in progress" */
  busy: boolean
  reload: () => void
}

/**
 * Async polling hook. Never blocks the UI: keeps the last good data on error/db_busy,
 * aborts in-flight requests on unmount/dep change, pauses while the tab is hidden.
 * `intervalMs = 0` -> fetch once (and on dep change / reload()).
 */
export function usePolling<T>(
  fetcher: (signal: AbortSignal) => Promise<T>,
  intervalMs: number,
  deps: unknown[] = [],
): PollState<T> {
  const [data, setData] = useState<T | null>(null)
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [error, setError] = useState<ApiError | Error | null>(null)
  const [tick, setTick] = useState(0)
  const fetcherRef = useRef(fetcher)
  fetcherRef.current = fetcher

  const reload = useCallback(() => setTick((n) => n + 1), [])

  useEffect(() => {
    let alive = true
    let timer: ReturnType<typeof setTimeout> | undefined
    let ctrl: AbortController | null = null

    const run = async () => {
      if (!alive) return
      if (typeof document !== 'undefined' && document.hidden) {
        if (intervalMs > 0) timer = setTimeout(run, intervalMs)
        return
      }
      ctrl = new AbortController()
      setRefreshing(true)
      try {
        const res = await fetcherRef.current(ctrl.signal)
        if (!alive) return
        setData(res)
        setError(null)
      } catch (e) {
        if (!alive || (e instanceof DOMException && e.name === 'AbortError')) return
        setError(e as Error)
      } finally {
        if (alive) {
          setLoading(false)
          setRefreshing(false)
          if (intervalMs > 0) timer = setTimeout(run, intervalMs)
        }
      }
    }
    void run()
    return () => {
      alive = false
      if (timer) clearTimeout(timer)
      ctrl?.abort()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [intervalMs, tick, ...deps])

  const busy = error instanceof ApiError && error.isDbBusy
  return { data, loading, refreshing, error, busy, reload }
}
