import type { ChatEvent, ToolStatus } from '../types/chat'

// Pure SSE parser: receives text chunks, returns complete events.
// Event = `event: <name>\ndata: <single-line JSON>\n\n`.
export function createSseParser() {
  let buffer = ''
  return {
    push(chunk: string): ChatEvent[] {
      buffer += chunk
      const events: ChatEvent[] = []
      const parts = buffer.split(/\r?\n\r?\n/)
      buffer = parts.pop() ?? ''
      for (const block of parts) {
        const ev = parseSseBlock(block)
        if (ev) events.push(ev)
      }
      return events
    },
  }
}

export function parseSseBlock(block: string): ChatEvent | null {
  let name = ''
  const dataLines: string[] = []
  for (const line of block.split(/\r?\n/)) {
    if (line.startsWith('event:')) name = line.slice(6).trim()
    else if (line.startsWith('data:')) dataLines.push(line.slice(5).trim())
  }
  if (!name || dataLines.length === 0) return null
  try {
    const d = JSON.parse(dataLines.join('\n')) as Record<string, unknown>
    switch (name) {
      case 'session':
        return { type: 'session', id: String(d.id) }
      case 'delta':
        return { type: 'delta', text: String(d.text ?? '') }
      case 'thinking':
        return { type: 'thinking', text: String(d.text ?? ''), tokens: Number(d.tokens ?? 0) }
      case 'tool':
        return {
          type: 'tool',
          name: String(d.name ?? ''),
          status: d.status as ToolStatus,
          id: String(d.id),
          input: typeof d.input === 'string' ? d.input : undefined,
        }
      case 'done':
        return {
          type: 'done',
          session_id: String(d.session_id),
          duration_ms: Number(d.duration_ms ?? 0),
          cost_usd: Number(d.cost_usd ?? 0),
        }
      case 'error':
        return { type: 'error', message: String(d.message ?? ''), code: String(d.code ?? '') }
      default:
        return null
    }
  } catch {
    return null
  }
}

// "mcp__chronoquant__get_candles" -> { server: "chronoquant", tool: "get_candles" }
export function splitToolName(full: string): { server: string; tool: string } {
  const m = /^mcp__([^_]+(?:-[^_]+)*)__(.+)$/.exec(full)
  return m ? { server: m[1], tool: m[2] } : { server: '', tool: full }
}

// The only writing chronoquant tool is enqueue_sync; everything else reads.
export function isWriteTool(full: string): boolean {
  const { server, tool } = splitToolName(full)
  return server === 'chronoquant' && tool === 'enqueue_sync'
}
