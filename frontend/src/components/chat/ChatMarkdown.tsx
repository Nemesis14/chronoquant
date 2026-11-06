import { Children, isValidElement, useState } from 'react'
import type { ReactElement, ReactNode } from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { Check, Copy } from 'lucide-react'

function textOf(node: ReactNode): string {
  if (typeof node === 'string' || typeof node === 'number') return String(node)
  if (Array.isArray(node)) return node.map(textOf).join('')
  if (isValidElement(node)) return textOf((node as ReactElement<{ children?: ReactNode }>).props.children)
  return ''
}

function CodeBlock({ children }: { children?: ReactNode }) {
  const [copied, setCopied] = useState(false)
  const code = Children.toArray(children).find(isValidElement) as
    | ReactElement<{ className?: string; children?: ReactNode }>
    | undefined
  const lang = /language-([\w-]+)/.exec(code?.props.className ?? '')?.[1]
  const text = textOf(code?.props.children ?? children).replace(/\n$/, '')

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(text)
      setCopied(true)
      setTimeout(() => setCopied(false), 1500)
    } catch {
      /* clipboard unavailable */
    }
  }

  return (
    <div className="my-2 rounded-md border border-cq-grid bg-black/40 overflow-hidden">
      <div className="flex items-center justify-between px-3 py-1 text-[11px] text-cq-muted bg-cq-grid/40">
        <span>{lang ?? 'kód'}</span>
        <button type="button" onClick={() => void copy()} className="flex items-center gap-1 hover:text-cq-text">
          {copied ? <Check size={12} /> : <Copy size={12} />}
          {copied ? 'Másolva' : 'Másol'}
        </button>
      </div>
      <pre className="overflow-x-auto p-3 text-[12.5px] leading-relaxed">
        <code>{text}</code>
      </pre>
    </div>
  )
}

/** Markdown (GFM) for assistant answers. Raw HTML is not rendered (react-markdown default). */
export default function ChatMarkdown({ text }: { text: string }) {
  return (
    <div className="chat-md text-sm leading-relaxed break-words">
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={{
          pre: ({ children }) => <CodeBlock>{children}</CodeBlock>,
          code: ({ className, children }) => (
            <code className={className ?? 'px-1 py-0.5 rounded bg-cq-grid/60 text-[12.5px]'}>{children}</code>
          ),
          a: ({ href, children }) => (
            <a href={href} target="_blank" rel="noopener noreferrer" className="text-cq-blue underline">
              {children}
            </a>
          ),
          p: ({ children }) => <p className="my-1.5">{children}</p>,
          ul: ({ children }) => <ul className="my-1.5 ml-5 list-disc space-y-0.5">{children}</ul>,
          ol: ({ children }) => <ol className="my-1.5 ml-5 list-decimal space-y-0.5">{children}</ol>,
          h1: ({ children }) => <h1 className="mt-3 mb-1 text-base font-bold">{children}</h1>,
          h2: ({ children }) => <h2 className="mt-3 mb-1 text-[15px] font-bold">{children}</h2>,
          h3: ({ children }) => <h3 className="mt-2 mb-1 text-sm font-semibold">{children}</h3>,
          blockquote: ({ children }) => (
            <blockquote className="my-2 border-l-2 border-cq-grid pl-3 text-cq-muted">{children}</blockquote>
          ),
          table: ({ children }) => (
            <div className="my-2 overflow-x-auto">
              <table className="min-w-full border-collapse text-[12.5px]">{children}</table>
            </div>
          ),
          th: ({ children }) => (
            <th className="border border-cq-grid bg-cq-grid/40 px-2 py-1 text-left font-semibold">{children}</th>
          ),
          td: ({ children }) => <td className="border border-cq-grid px-2 py-1">{children}</td>,
          hr: () => <hr className="my-3 border-cq-grid" />,
        }}
      >
        {text}
      </ReactMarkdown>
    </div>
  )
}
