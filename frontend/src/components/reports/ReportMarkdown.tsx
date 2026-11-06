import { Fragment, useMemo } from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import type { ChartRef, TableRef } from '../../types/reports'
import ReportChart from './ReportChart'
import ReportTable from './ReportTable'

type Block = { kind: 'md'; text: string } | { kind: 'chart' | 'table'; id: string }

const PLACEHOLDER = /^\{\{(chart|table):([a-z0-9][a-z0-9_-]*)\}\}\s*$/
const FENCE = /^\s*(```|~~~)/

/** Split report.md into markdown chunks and own-line `{{chart:id}}` / `{{table:id}}` placeholders. */
export function splitBlocks(markdown: string): Block[] {
  const blocks: Block[] = []
  let buf: string[] = []
  let fenced = false
  const flush = () => {
    const text = buf.join('\n')
    if (text.trim()) blocks.push({ kind: 'md', text })
    buf = []
  }
  for (const line of markdown.split('\n')) {
    if (FENCE.test(line)) fenced = !fenced
    const m = fenced ? null : PLACEHOLDER.exec(line)
    if (m) {
      flush()
      blocks.push({ kind: m[1] as 'chart' | 'table', id: m[2] })
    } else {
      buf.push(line)
    }
  }
  flush()
  return blocks
}

function Md({ text }: { text: string }) {
  return (
    <div className="report-md break-words text-[14px] leading-relaxed">
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={{
          a: ({ href, children }) => (
            <a href={href} target="_blank" rel="noopener noreferrer" className="text-cq-blue underline">
              {children}
            </a>
          ),
          p: ({ children }) => <p className="my-2">{children}</p>,
          ul: ({ children }) => <ul className="my-2 ml-6 list-disc space-y-0.5">{children}</ul>,
          ol: ({ children }) => <ol className="my-2 ml-6 list-decimal space-y-0.5">{children}</ol>,
          h1: ({ children }) => <h1 className="mb-2 mt-6 text-xl font-bold">{children}</h1>,
          h2: ({ children }) => (
            <h2 className="mb-2 mt-7 border-b border-cq-grid pb-1 text-lg font-bold">{children}</h2>
          ),
          h3: ({ children }) => <h3 className="mb-1 mt-5 text-[15px] font-semibold">{children}</h3>,
          h4: ({ children }) => <h4 className="mb-1 mt-4 text-sm font-semibold">{children}</h4>,
          blockquote: ({ children }) => (
            <blockquote className="my-2 border-l-2 border-cq-grid pl-3 text-cq-muted">{children}</blockquote>
          ),
          code: ({ className, children }) => (
            <code className={className ?? 'rounded bg-cq-grid/60 px-1 py-0.5 text-[12.5px]'}>{children}</code>
          ),
          pre: ({ children }) => (
            <pre className="my-2 overflow-x-auto rounded-md border border-cq-grid bg-black/40 p-3 text-[12.5px]">
              {children}
            </pre>
          ),
          table: ({ children }) => (
            <div className="my-3 overflow-x-auto">
              <table className="min-w-full border-collapse text-[12.5px]">{children}</table>
            </div>
          ),
          th: ({ children }) => (
            <th className="border border-cq-grid bg-cq-grid/40 px-2 py-1 text-left font-semibold">{children}</th>
          ),
          td: ({ children }) => <td className="border border-cq-grid px-2 py-1">{children}</td>,
          hr: () => <hr className="my-4 border-cq-grid" />,
        }}
      >
        {text}
      </ReactMarkdown>
    </div>
  )
}

interface Props {
  owner: string
  slug: string
  markdown: string
  charts: ChartRef[]
  tables: TableRef[]
}

/** Renders report.md; placeholders become lazy chart / table components. */
export default function ReportMarkdown({ owner, slug, markdown, charts, tables }: Props) {
  const blocks = useMemo(() => splitBlocks(markdown), [markdown])
  const chartById = useMemo(() => new Map(charts.map((c) => [c.id, c])), [charts])
  const tableById = useMemo(() => new Map(tables.map((t) => [t.id, t])), [tables])

  const used = new Set(blocks.flatMap((b) => (b.kind === 'md' ? [] : [b.id])))
  const extraCharts = charts.filter((c) => !used.has(c.id))
  const extraTables = tables.filter((t) => !used.has(t.id))

  return (
    <>
      {blocks.map((b, i) => {
        if (b.kind === 'md') return <Md key={i} text={b.text} />
        if (b.kind === 'chart') {
          const ref = chartById.get(b.id)
          return ref ? (
            <ReportChart key={i} owner={owner} slug={slug} chart={ref} />
          ) : (
            <div key={i} className="text-xs text-cq-red">
              Hiányzó chart: {b.id}
            </div>
          )
        }
        const ref = tableById.get(b.id)
        return ref ? (
          <ReportTable key={i} owner={owner} slug={slug} table={ref} />
        ) : (
          <div key={i} className="text-xs text-cq-red">
            Hiányzó tábla: {b.id}
          </div>
        )
      })}
      {(extraCharts.length > 0 || extraTables.length > 0) && (
        <Fragment>
          <h2 className="mb-2 mt-7 border-b border-cq-grid pb-1 text-lg font-bold">Mellékletek</h2>
          {extraCharts.map((c) => (
            <ReportChart key={c.id} owner={owner} slug={slug} chart={c} />
          ))}
          {extraTables.map((t) => (
            <ReportTable key={t.id} owner={owner} slug={slug} table={t} />
          ))}
        </Fragment>
      )}
    </>
  )
}
