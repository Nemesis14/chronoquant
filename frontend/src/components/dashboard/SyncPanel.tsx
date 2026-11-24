import { useEffect, useRef, useState } from 'react'
import { ApiError } from '../../api/client'
import { cancelJob, enqueueSync, getAssets, getAutoSync, listJobs, setAutoSync } from '../../api/dashboard'
import { useJobStream } from '../../lib/jobStream'
import { useAsset } from '../../lib/asset'
import { usePolling } from '../../lib/usePolling'
import { ago, errMessage, fullTime, shortTime } from '../../lib/format'
import type { Job, Status } from '../../types/api'
import { Card, Dot, Muted, PollNote, Spinner } from './ui'

const STATUS_COLOR: Record<string, string> = {
  queued: 'text-cq-gold',
  running: 'text-cq-blue',
  succeeded: 'text-cq-green',
  failed: 'text-cq-red',
  canceled: 'text-cq-muted',
}

function JobLog({ jobId }: { jobId: string }) {
  const stream = useJobStream(jobId)
  const box = useRef<HTMLDivElement>(null)
  useEffect(() => {
    const el = box.current
    if (el) el.scrollTop = el.scrollHeight
  }, [stream.lines.length])
  return (
    <div>
      <div
        ref={box}
        className="bg-cq-bg border border-cq-grid rounded p-2 h-40 overflow-auto font-mono text-[11px] leading-snug whitespace-pre-wrap break-all"
      >
        {stream.lines.length ? stream.lines.join('\n') : <span className="text-cq-muted">Nincs log sor.</span>}
      </div>
      <div className="text-[11px] text-cq-muted mt-1 flex gap-2">
        <span>SSE: {stream.done ? 'lezárva' : stream.connError ? 'újracsatlakozás…' : 'élő'}</span>
        {stream.done?.error && <span className="text-cq-red">{stream.done.error}</span>}
      </div>
    </div>
  )
}

export default function SyncPanel({ status, onChanged }: { status: Status | null; onChanged: () => void }) {
  const asset = useAsset()
  const running = status?.sync.running ?? false
  const assets = usePolling((s) => getAssets(s), 0)
  const defaultAsset = assets.data?.default_asset_id ?? 'solusdt'
  const jobs = usePolling((s) => listJobs(40, s), running ? 2000 : 8000, [running])
  const auto = usePolling((s) => getAutoSync(s), 4000)
  const [busy, setBusy] = useState(false)
  const [msg, setMsg] = useState<{ kind: 'err' | 'info'; text: string } | null>(null)
  const [selected, setSelected] = useState<string | null>(null)
  const [showLog, setShowLog] = useState(true)

  // the job list is global: show only this asset's jobs (legacy jobs without asset_id = default asset)
  const list = (jobs.data?.items ?? [])
    .filter((j) => String(j.params?.asset_id ?? defaultAsset) === asset)
    .slice(0, 8)
  const active = list.find((j) => j.status === 'running') ?? list.find((j) => j.status === 'queued')
  const logJobId = selected ?? active?.id ?? list[0]?.id ?? null
  const last: Job | undefined = list.find((j) => j.status === 'succeeded' || j.status === 'failed')

  const act = async (fn: () => Promise<unknown>, okText?: string) => {
    setBusy(true)
    setMsg(null)
    try {
      await fn()
      if (okText) setMsg({ kind: 'info', text: okText })
      jobs.reload()
      auto.reload()
      onChanged()
    } catch (e) {
      const text =
        e instanceof ApiError && e.code === 'worker_down'
          ? 'A worker nem fut — indítsd el (scripts/start_worker.ps1), majd próbáld újra.'
          : errMessage(e)
      setMsg({ kind: 'err', text })
    } finally {
      setBusy(false)
    }
  }

  const workerAlive = status?.worker.alive ?? false

  return (
    <Card
      title="Szinkron"
      right={
        <>
          {(running || jobs.refreshing) && <Spinner />}
          <span className="flex items-center gap-1">
            <Dot ok={workerAlive} /> worker {workerAlive ? 'él' : 'áll'}
          </span>
        </>
      }
    >
      <div className="space-y-2 text-xs">
        {running ? (
          <div className="text-cq-blue">Szinkron fut… (a felület közben használható)</div>
        ) : last ? (
          <div className={STATUS_COLOR[last.status]}>
            Utolsó: {last.status} · {ago(last.finished_at)} ezelőtt
            {last.result?.inserted_ohlcv_rows !== undefined && ` · +${last.result.inserted_ohlcv_rows} OHLCV sor`}
          </div>
        ) : (
          <Muted>Szinkron üresjáratban</Muted>
        )}

        <div className="flex gap-2">
          <button
            disabled={busy || running || !workerAlive}
            onClick={() => void act(() => enqueueSync(asset, 'sync_full'), 'Sync sorba állítva')}
            className="flex-1 px-3 py-1.5 rounded bg-cq-blue/90 hover:bg-cq-blue text-white text-xs font-semibold disabled:opacity-40"
          >
            Sync
          </button>
          {active?.status === 'queued' && (
            <button
              disabled={busy}
              onClick={() => void act(() => cancelJob(active.id), 'Job törölve')}
              className="px-3 py-1.5 rounded border border-cq-grid text-xs hover:bg-white/5"
            >
              Törlés
            </button>
          )}
        </div>
        {!workerAlive && <div className="text-cq-gold">A worker nem fut, a Sync nem indítható.</div>}

        <label className="flex items-center gap-2 cursor-pointer select-none">
          <input
            type="checkbox"
            checked={auto.data?.enabled ?? false}
            disabled={busy || !auto.data}
            onChange={(e) => void act(() => setAutoSync(e.target.checked))}
          />
          <span>Auto-sync (perc zárás + {auto.data?.delay_seconds ?? 3} mp)</span>
          {auto.data?.enabled && auto.data.next_due_in_s !== null && (
            <span className="text-cq-muted ml-auto">{auto.data.next_due_in_s} mp</span>
          )}
        </label>

        {msg && <div className={msg.kind === 'err' ? 'text-cq-red' : 'text-cq-muted'}>{msg.text}</div>}
        <PollNote poll={jobs} what="jobs" />
        <PollNote poll={auto} what="auto-sync" />

        {list.length > 0 && (
          <div className="border-t border-cq-grid pt-2">
            <div className="flex items-center justify-between mb-1">
              <span className="text-cq-muted">Jobok</span>
              <button className="text-cq-muted hover:text-cq-text" onClick={() => setShowLog((v) => !v)}>
                {showLog ? 'log elrejt' : 'log mutat'}
              </button>
            </div>
            <ul className="space-y-0.5 max-h-28 overflow-auto">
              {list.map((j) => (
                <li
                  key={j.id}
                  onClick={() => setSelected(j.id)}
                  className={`flex justify-between gap-2 px-1 rounded cursor-pointer hover:bg-white/5 ${j.id === logJobId ? 'bg-white/10' : ''}`}
                  title={`${j.id}${j.error ? `\n${j.error}` : ''}`}
                >
                  <span>{shortTime(j.created_at)}</span>
                  <span className="text-cq-muted">{j.kind}</span>
                  <span className={STATUS_COLOR[j.status]}>{j.status}</span>
                </li>
              ))}
            </ul>
            {showLog && logJobId && (
              <div className="mt-2">
                <div className="text-[11px] text-cq-muted mb-1">
                  Log: {logJobId}
                  {list.find((j) => j.id === logJobId)?.started_at &&
                    ` · ${fullTime(list.find((j) => j.id === logJobId)?.started_at)}`}
                </div>
                <JobLog key={logJobId} jobId={logJobId} />
              </div>
            )}
          </div>
        )}
      </div>
    </Card>
  )
}
