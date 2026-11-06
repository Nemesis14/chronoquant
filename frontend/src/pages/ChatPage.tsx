import { useSearchParams } from 'react-router-dom'
import ChatPanel from '../components/chat/ChatPanel'
import { openChatPopout } from '../lib/popout'
import { useChat } from '../lib/useChat'

/**
 * /chat — full-page chat. `/chat?popout=1` (opened by the popout button) covers the app layout
 * (nav) so the window shows only the chat.
 */
export default function ChatPage() {
  const [params] = useSearchParams()
  const popout = params.get('popout') === '1'
  const chat = useChat()

  if (popout) {
    return (
      <div className="fixed inset-0 z-50 bg-cq-bg">
        <ChatPanel chat={chat} variant="page" />
      </div>
    )
  }
  return (
    <div className="h-[calc(100vh-110px)] min-h-[420px] overflow-hidden rounded-xl border border-cq-grid">
      <ChatPanel chat={chat} variant="page" onPopout={openChatPopout} />
    </div>
  )
}
