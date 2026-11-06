import { ApiError } from './client'
import api from './client'
import { createSseParser } from '../lib/chatStream'
import type { ChatConfig, ChatEvent } from '../types/chat'

export class ChatHttpError extends ApiError {}

/**
 * POST /api/chat and stream the SSE response (fetch + ReadableStream: EventSource cannot POST).
 * Abort via `signal`: the server kills the claude process tree when the connection drops.
 */
export async function streamChat(
  message: string,
  sessionId: string | null,
  onEvent: (ev: ChatEvent) => void,
  signal: AbortSignal,
): Promise<void> {
  const res = await fetch('/api/chat', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message, session_id: sessionId }),
    signal,
  })
  if (!res.ok) {
    let detail = res.statusText
    let code = 'error'
    try {
      const body = await res.json()
      if (typeof body?.detail === 'string') detail = body.detail
      else if (Array.isArray(body?.detail)) detail = 'invalid request'
      if (typeof body?.code === 'string') code = body.code
    } catch {
      /* non-JSON error body */
    }
    throw new ChatHttpError(res.status, code, detail)
  }
  if (!res.body) throw new ChatHttpError(res.status, 'no_body', 'empty response body')
  const reader = res.body.getReader()
  const decoder = new TextDecoder()
  const parser = createSseParser()
  for (;;) {
    const { done, value } = await reader.read()
    if (done) break
    for (const ev of parser.push(decoder.decode(value, { stream: true }))) onEvent(ev)
  }
}

export const getChatConfig = (signal?: AbortSignal) => api.get<ChatConfig>('/api/chat/config', undefined, signal)
