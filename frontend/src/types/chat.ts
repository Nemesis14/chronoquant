// Chat types (SSE contract: _doc_/_plans_/068_bff_ui.md §4.2).

export type ToolStatus = 'start' | 'done' | 'error'

export type ChatEvent =
  | { type: 'session'; id: string }
  | { type: 'delta'; text: string }
  | { type: 'thinking'; text: string; tokens: number }
  | { type: 'tool'; name: string; status: ToolStatus; id: string; input?: string }
  | { type: 'done'; session_id: string; duration_ms: number; cost_usd: number }
  | { type: 'error'; message: string; code: string }

export interface ChatToolUse {
  id: string
  name: string
  status: ToolStatus
  input?: string
}

export interface ChatMessage {
  role: 'user' | 'assistant'
  text: string
  tools: ChatToolUse[]
  thinking?: string
  thinkingTokens?: number
  error?: string
  /** assistant turn only: duration of the finished turn */
  durationMs?: number
}

export interface ChatSessionMeta {
  id: string
  title: string
  updated_at: number
}

export interface ChatConfig {
  model: string
  timeout_s: number
  tools: string[]
  mcp_ok: boolean
  binary_ok?: boolean
}
