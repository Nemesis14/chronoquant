import { useEffect, useRef, useState } from 'react'
import { getLogs } from '../../api/dashboard'
import { usePolling } from '../../lib/usePolling'
import { Muted, PollNote } from './ui'

const LEVEL: Record<string, string> = {
  INFO: 'text-cq-text',
  WARNING: 'text-cq-gold',
  ERROR: 'text-cq-red',
  CRITICAL: 'text-cq-red',
  DEBUG: 'text-cq-muted',
}

/** Read-only log tail (port of ui/components/log_panel.py), API or worker log. */
export default function LogPanel() {
  const [source, setSource] = useState<'api' | 'worker'>('api')
  const poll = usePolling((s) => getLogs(source, 300, s), 5000, [source])
  const box = useRef<HTMLDivElement>(null)
  const lines = poll.data?.lines ?? []
  useEffect(() => {
    const el = box.current
    if (el) el.scrollTop = el.scrollHeight
  }, [lines.length, source])

  return (
    <div>
      <div className="flex gap-2 mb-2 text-xs">
        {(['api', 'worker'] as const).map((s) => (
          <button
            key={s}
            onClick={() => setSource(s)}
            className={`px-2 py-0.5 rounded border ${source === s ? 'border-cq-blue text-cq-blue' : 'border-cq-grid text-cq-muted'}`}
          >
            {s}
          </button>
        ))}
        <button className="ml-auto text-cq-muted hover:text-cq-text" onClick={poll.reload}>
          frissít
        </button>
      </div>
      <div
        ref={box}
        className="bg-cq-bg border border-cq-grid rounded p-2 h-56 overflow-auto font-mono text-xs leading-relaxed"
      >
        {lines.length === 0 && !poll.loading && <Muted>Nincs log.</Muted>}
        {lines.map((l, i) => (
          <div key={i} className="whitespace-pre-wrap break-all">
            <span className="text-gray-600 mr-2">{(l.ts ?? '').slice(0, 16).replace('T', ' ')}</span>
            <span className={`${LEVEL[(l.level ?? 'INFO').toUpperCase()] ?? 'text-cq-text'}`}>
              <b className="mr-2">[{(l.level ?? 'INFO').slice(0, 4).toUpperCase()}]</b>
              {l.msg}
            </span>
          </div>
        ))}
      </div>
      <PollNote poll={poll} what="log" />
    </div>
  )
}
