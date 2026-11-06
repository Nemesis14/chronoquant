import { useEffect, useRef, useState } from 'react'
import { ExternalLink, History, MessageSquare, Plus, Send, Square, Trash2, X } from 'lucide-react'
import ChatMarkdown from './ChatMarkdown'
import { isWriteTool, splitToolName } from '../../lib/chatStream'
import type { UseChat } from '../../lib/useChat'
import type { ChatMessage, ChatToolUse } from '../../types/chat'

function ToolChip({ tool }: { tool: ChatToolUse }) {
  const { server, tool: name } = splitToolName(tool.name)
  const verb = isWriteTool(tool.name) ? 'ír' : server ? 'olvas' : 'eszköz'
  const tone =
    tool.status === 'error'
      ? 'border-cq-red/60 text-cq-red'
      : tool.status === 'start'
        ? 'border-cq-gold/60 text-cq-gold animate-pulse'
        : 'border-cq-grid text-cq-muted'
  return (
    <span
      title={tool.input || tool.name}
      className={`inline-flex max-w-full items-center gap-1 truncate rounded-full border px-2 py-0.5 text-[11px] ${tone}`}
    >
      <span className="shrink-0">{verb}:</span>
      <span className="truncate">{name}</span>
      {tool.status === 'error' && <span className="shrink-0">(hiba)</span>}
    </span>
  )
}

function AssistantBubble({ m, live }: { m: ChatMessage; live: boolean }) {
  return (
    <div className="max-w-[95%] rounded-lg bg-cq-panel border border-cq-grid px-3 py-2">
      {m.tools.length > 0 && (
        <div className="mb-2 flex flex-wrap gap-1">
          {m.tools.map((t) => (
            <ToolChip key={t.id} tool={t} />
          ))}
        </div>
      )}
      {m.thinking && (
        <details className="mb-2 text-xs text-cq-muted">
          <summary className="cursor-pointer select-none">
            Gondolkodás{m.thinkingTokens ? ` (~${m.thinkingTokens} token)` : ''}
          </summary>
          <p className="mt-1 whitespace-pre-wrap">{m.thinking}</p>
        </details>
      )}
      {m.text && <ChatMarkdown text={m.text} />}
      {live && !m.error && (
        <span className="ml-1 inline-block animate-pulse text-cq-muted">{m.text ? '▍' : 'Gondolkodom…'}</span>
      )}
      {m.error && <div className="mt-2 text-xs text-cq-red">{m.error}</div>}
    </div>
  )
}

interface Props {
  chat: UseChat
  variant: 'page' | 'dock'
  onClose?: () => void
  onPopout?: () => void
}

/** The chat UI (thread + composer + session list). Used by ChatPage and by the ChatDock. */
export default function ChatPanel({ chat, variant, onClose, onPopout }: Props) {
  const { sessions, activeId, messages, busy, send, stop, newConversation, selectSession, removeSession } = chat
  const [input, setInput] = useState('')
  const [showSessions, setShowSessions] = useState(variant === 'page')
  const bottomRef = useRef<HTMLDivElement>(null)
  const stickRef = useRef(true)
  const scrollRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (stickRef.current) bottomRef.current?.scrollIntoView({ block: 'end' })
  }, [messages])

  const onScroll = () => {
    const el = scrollRef.current
    if (el) stickRef.current = el.scrollHeight - el.scrollTop - el.clientHeight < 80
  }

  const submit = () => {
    const text = input.trim()
    if (!text || busy) return
    setInput('')
    stickRef.current = true
    void send(text)
  }

  const sessionList = (
    <aside
      className={
        variant === 'page'
          ? 'flex w-64 shrink-0 flex-col border-r border-cq-grid'
          : 'absolute inset-x-0 top-[49px] z-10 max-h-[60%] overflow-y-auto border-b border-cq-grid bg-cq-bg shadow-lg'
      }
    >
      <div className="p-2">
        <button
          type="button"
          onClick={() => {
            newConversation()
            if (variant === 'dock') setShowSessions(false)
          }}
          className="flex w-full items-center justify-center gap-1 rounded-lg border border-cq-grid px-2 py-1.5 text-xs text-cq-text hover:bg-cq-grid/40"
        >
          <Plus size={14} /> Új beszélgetés
        </button>
      </div>
      <ul className="flex-1 overflow-y-auto px-2 pb-2">
        {sessions.length === 0 && <li className="px-2 py-3 text-xs text-cq-muted">Még nincs mentett beszélgetés.</li>}
        {sessions.map((s) => (
          <li key={s.id} className="group relative">
            <button
              type="button"
              onClick={() => {
                selectSession(s.id)
                if (variant === 'dock') setShowSessions(false)
              }}
              title={s.title}
              className={`flex w-full items-center gap-2 truncate rounded-md px-2 py-1.5 pr-7 text-left text-xs ${
                s.id === activeId ? 'bg-cq-grid/60 text-cq-text' : 'text-cq-muted hover:bg-cq-grid/30'
              }`}
            >
              <MessageSquare size={12} className="shrink-0" />
              <span className="truncate">{s.title}</span>
            </button>
            <button
              type="button"
              onClick={() => removeSession(s.id)}
              title="Törlés (csak a helyi előzmény)"
              className="absolute right-1 top-1/2 hidden -translate-y-1/2 rounded p-1 text-cq-muted hover:text-cq-red group-hover:block"
            >
              <Trash2 size={12} />
            </button>
          </li>
        ))}
      </ul>
    </aside>
  )

  return (
    <div className="relative flex h-full min-h-0 flex-col bg-cq-bg text-cq-text">
      <header className="flex items-center justify-between border-b border-cq-grid px-3 py-2.5">
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => setShowSessions((v) => !v)}
            title="Beszélgetések"
            className="rounded p-1 text-cq-muted hover:bg-cq-grid/40 hover:text-cq-text"
          >
            <History size={16} />
          </button>
          <h2 className="text-sm font-semibold">ChronoQuant chat</h2>
        </div>
        <div className="flex items-center gap-1">
          {variant === 'dock' && (
            <button
              type="button"
              onClick={newConversation}
              title="Új beszélgetés"
              className="rounded p-1 text-cq-muted hover:bg-cq-grid/40 hover:text-cq-text"
            >
              <Plus size={16} />
            </button>
          )}
          {onPopout && (
            <button
              type="button"
              onClick={onPopout}
              title="Megnyitás külön ablakban"
              className="rounded p-1 text-cq-muted hover:bg-cq-grid/40 hover:text-cq-text"
            >
              <ExternalLink size={16} />
            </button>
          )}
          {onClose && (
            <button
              type="button"
              onClick={onClose}
              title="Bezár"
              className="rounded p-1 text-cq-muted hover:bg-cq-grid/40 hover:text-cq-text"
            >
              <X size={16} />
            </button>
          )}
        </div>
      </header>

      <div className="flex min-h-0 flex-1">
        {showSessions && sessionList}
        <div className="flex min-w-0 flex-1 flex-col">
          <div ref={scrollRef} onScroll={onScroll} className="flex-1 space-y-3 overflow-y-auto px-4 py-3">
            {messages.length === 0 && (
              <p className="text-sm text-cq-muted">
                Kérdezz a piaci adatokról, predikciókról, stratégiáról, trade-ekről vagy az elemzési riportokról.
                {activeId && ' (A korábbi beszélgetés folytatódik.)'}
              </p>
            )}
            {messages.map((m, i) =>
              m.role === 'user' ? (
                <div key={i} className="flex justify-end">
                  <div className="max-w-[85%] whitespace-pre-wrap break-words rounded-lg bg-cq-blue/25 px-3 py-2 text-sm">
                    {m.text}
                  </div>
                </div>
              ) : (
                <div key={i} className="flex justify-start">
                  <AssistantBubble m={m} live={busy && i === messages.length - 1} />
                </div>
              ),
            )}
            <div ref={bottomRef} />
          </div>

          <form
            onSubmit={(e) => {
              e.preventDefault()
              submit()
            }}
            className="flex items-end gap-2 border-t border-cq-grid p-3"
          >
            <textarea
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter' && !e.shiftKey && !e.nativeEvent.isComposing) {
                  e.preventDefault()
                  submit()
                }
              }}
              rows={2}
              maxLength={8000}
              placeholder="Írj üzenetet… (Enter: küldés, Shift+Enter: új sor)"
              className="flex-1 resize-none rounded-lg border border-cq-grid bg-cq-panel px-3 py-2 text-sm text-cq-text placeholder:text-cq-muted focus:border-cq-blue focus:outline-none"
            />
            {busy ? (
              <button
                type="button"
                onClick={stop}
                title="Megszakítás"
                className="rounded-lg bg-cq-red/80 p-2 text-white hover:bg-cq-red"
              >
                <Square size={16} />
              </button>
            ) : (
              <button
                type="submit"
                disabled={!input.trim()}
                title="Küldés"
                className="rounded-lg bg-cq-blue p-2 text-white hover:opacity-90 disabled:opacity-40"
              >
                <Send size={16} />
              </button>
            )}
          </form>
        </div>
      </div>
    </div>
  )
}
