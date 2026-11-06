// Client-side chat persistence (the API stores no conversations; the claude CLI keeps the
// server-side history behind `--resume <id>`). Every localStorage access is guarded.
import type { ChatMessage, ChatSessionMeta } from '../types/chat'

const SESSIONS_KEY = 'cq_chat_sessions'
const ACTIVE_KEY = 'cq_chat_active'
const MSG_PREFIX = 'cq_chat_msgs_'
const MAX_SESSIONS = 50
const MAX_MESSAGES = 200
export const CHANNEL = 'cq-chat'

function read<T>(key: string, fallback: T): T {
  try {
    const raw = localStorage.getItem(key)
    return raw ? (JSON.parse(raw) as T) : fallback
  } catch {
    return fallback
  }
}

function write(key: string, value: unknown) {
  try {
    localStorage.setItem(key, JSON.stringify(value))
  } catch {
    /* quota / unavailable */
  }
}

export function loadSessions(): ChatSessionMeta[] {
  const list = read<ChatSessionMeta[]>(SESSIONS_KEY, [])
  return Array.isArray(list) ? [...list].sort((a, b) => b.updated_at - a.updated_at) : []
}

export function upsertSession(id: string, title: string): ChatSessionMeta[] {
  const list = loadSessions().filter((s) => s.id !== id)
  const existing = loadSessions().find((s) => s.id === id)
  list.unshift({ id, title: existing?.title || title, updated_at: Date.now() })
  const trimmed = list.slice(0, MAX_SESSIONS)
  for (const dropped of list.slice(MAX_SESSIONS)) removeMessages(dropped.id)
  write(SESSIONS_KEY, trimmed)
  return trimmed
}

export function deleteSession(id: string): ChatSessionMeta[] {
  const list = loadSessions().filter((s) => s.id !== id)
  write(SESSIONS_KEY, list)
  removeMessages(id)
  return list
}

export function loadMessages(id: string): ChatMessage[] {
  const list = read<ChatMessage[]>(MSG_PREFIX + id, [])
  return Array.isArray(list) ? list : []
}

export function saveMessages(id: string, messages: ChatMessage[]) {
  write(MSG_PREFIX + id, messages.slice(-MAX_MESSAGES))
}

function removeMessages(id: string) {
  try {
    localStorage.removeItem(MSG_PREFIX + id)
  } catch {
    /* ignore */
  }
}

export function loadActiveId(): string | null {
  try {
    return localStorage.getItem(ACTIVE_KEY)
  } catch {
    return null
  }
}

export function saveActiveId(id: string | null) {
  try {
    if (id) localStorage.setItem(ACTIVE_KEY, id)
    else localStorage.removeItem(ACTIVE_KEY)
  } catch {
    /* ignore */
  }
}

export function makeTitle(text: string): string {
  const t = text.replace(/\s+/g, ' ').trim()
  return t.length > 60 ? t.slice(0, 57) + '...' : t || 'Új beszélgetés'
}
