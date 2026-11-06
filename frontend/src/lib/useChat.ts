import { useCallback, useEffect, useRef, useState } from 'react'
import { ChatHttpError, streamChat } from '../api/chat'
import {
  CHANNEL,
  deleteSession,
  loadActiveId,
  loadMessages,
  loadSessions,
  makeTitle,
  saveActiveId,
  saveMessages,
  upsertSession,
} from './chatStore'
import type { ChatEvent, ChatMessage, ChatSessionMeta } from '../types/chat'

function httpErrorMessage(e: ChatHttpError): string {
  if (e.code === 'binary_not_found')
    return 'A `claude` program nem található a gépen. Telepítsd, vagy add meg az útvonalát a CLAUDE_BIN környezeti változóban.'
  if (e.status === 429 || e.code === 'busy') return 'Már fut két chat-kérés; próbáld újra pár másodperc múlva.'
  if (e.status === 422) return 'Az üzenet érvénytelen (1-8000 karakter lehet).'
  return `A chat nem érhető el (HTTP ${e.status}). ${e.message}`.trim()
}

const CLI_ERROR_HINT: Record<string, string> = {
  timeout: 'Időtúllépés - a válasz megszakadt.',
}

export interface UseChat {
  sessions: ChatSessionMeta[]
  activeId: string | null
  messages: ChatMessage[]
  busy: boolean
  send: (text: string) => Promise<void>
  stop: () => void
  newConversation: () => void
  selectSession: (id: string) => void
  removeSession: (id: string) => void
}

/** Chat state machine: streaming turn, stop, session list, resume. Shared by page and dock. */
export function useChat(): UseChat {
  const [sessions, setSessions] = useState<ChatSessionMeta[]>(loadSessions)
  const [activeId, setActiveId] = useState<string | null>(loadActiveId)
  const [messages, setMessages] = useState<ChatMessage[]>(() => {
    const id = loadActiveId()
    return id ? loadMessages(id) : []
  })
  const [busy, setBusy] = useState(false)
  const abortRef = useRef<AbortController | null>(null)
  const turnRef = useRef(0)
  const messagesRef = useRef(messages)
  const activeRef = useRef(activeId)
  const channelRef = useRef<BroadcastChannel | null>(null)

  messagesRef.current = messages
  activeRef.current = activeId

  useEffect(() => {
    if (typeof BroadcastChannel === 'undefined') return
    const ch = new BroadcastChannel(CHANNEL)
    channelRef.current = ch
    ch.onmessage = () => setSessions(loadSessions())
    return () => {
      ch.close()
      channelRef.current = null
    }
  }, [])

  useEffect(() => () => abortRef.current?.abort(), [])

  const refreshSessions = useCallback((list: ChatSessionMeta[]) => {
    setSessions(list)
    channelRef.current?.postMessage('sessions')
  }, [])

  const stop = useCallback(() => abortRef.current?.abort(), [])

  const invalidateTurn = useCallback(() => {
    turnRef.current += 1
    abortRef.current?.abort()
  }, [])

  const newConversation = useCallback(() => {
    invalidateTurn()
    setBusy(false)
    setActiveId(null)
    saveActiveId(null)
    setMessages([])
  }, [invalidateTurn])

  const selectSession = useCallback(
    (id: string) => {
      invalidateTurn()
      setBusy(false)
      setActiveId(id)
      saveActiveId(id)
      setMessages(loadMessages(id))
    },
    [invalidateTurn],
  )

  const removeSession = useCallback(
    (id: string) => {
      refreshSessions(deleteSession(id))
      if (activeRef.current === id) newConversation()
    },
    [newConversation, refreshSessions],
  )

  const send = useCallback(
    async (raw: string) => {
      const text = raw.trim()
      if (!text || abortRef.current) return
      const turn = ++turnRef.current
      let turnMsgs: ChatMessage[] = [
        ...messagesRef.current,
        { role: 'user', text, tools: [] },
        { role: 'assistant', text: '', tools: [] },
      ]
      let sid: string | null = activeRef.current
      const title = makeTitle(text)
      const current = () => turnRef.current === turn
      const patchLast = (fn: (m: ChatMessage) => ChatMessage) => {
        turnMsgs = [...turnMsgs.slice(0, -1), fn(turnMsgs[turnMsgs.length - 1])]
        if (current()) setMessages(turnMsgs)
      }
      const adoptSession = (id: string) => {
        sid = id
        refreshSessions(upsertSession(id, title))
        if (current()) {
          setActiveId(id)
          saveActiveId(id)
        }
      }

      setMessages(turnMsgs)
      setBusy(true)
      const ctrl = new AbortController()
      abortRef.current = ctrl

      const handle = (ev: ChatEvent) => {
        switch (ev.type) {
          case 'session':
            adoptSession(ev.id)
            break
          case 'delta':
            patchLast((m) => ({ ...m, text: m.text + ev.text }))
            break
          case 'thinking':
            patchLast((m) => ({
              ...m,
              thinking: (m.thinking ?? '') + ev.text,
              thinkingTokens: Math.max(m.thinkingTokens ?? 0, ev.tokens),
            }))
            break
          case 'tool':
            patchLast((m) => {
              const exists = m.tools.some((t) => t.id === ev.id)
              const tools = exists
                ? m.tools.map((t) =>
                    t.id === ev.id
                      ? { ...t, name: ev.name || t.name, status: ev.status, input: ev.input ?? t.input }
                      : t,
                  )
                : [...m.tools, { id: ev.id, name: ev.name, status: ev.status, input: ev.input }]
              return { ...m, tools }
            })
            break
          case 'done':
            adoptSession(ev.session_id)
            patchLast((m) => ({ ...m, durationMs: ev.duration_ms }))
            break
          case 'error':
            patchLast((m) => ({
              ...m,
              error: CLI_ERROR_HINT[ev.code] ?? (ev.message || 'A chat hibával leállt.'),
            }))
            break
        }
      }

      try {
        await streamChat(text, sid, handle, ctrl.signal)
      } catch (e) {
        if (ctrl.signal.aborted) {
          patchLast((m) => ({ ...m, error: 'Megszakítva.' }))
        } else if (e instanceof ChatHttpError) {
          patchLast((m) => ({ ...m, error: httpErrorMessage(e) }))
        } else {
          patchLast((m) => ({ ...m, error: 'Nem érhető el a szerver (offline?). Ellenőrizd, hogy a ChronoQuant API fut.' }))
        }
      } finally {
        if (abortRef.current === ctrl) abortRef.current = null
        if (sid) saveMessages(sid, turnMsgs)
        if (current()) setBusy(false)
      }
    },
    [refreshSessions],
  )

  return { sessions, activeId, messages, busy, send, stop, newConversation, selectSession, removeSession }
}
