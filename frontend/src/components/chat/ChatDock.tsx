import { useState } from 'react'
import { useLocation } from 'react-router-dom'
import { MessageSquare } from 'lucide-react'
import ChatPanel from './ChatPanel'
import { openChatPopout } from '../../lib/popout'
import { useChat } from '../../lib/useChat'

const OPEN_KEY = 'cq_chat_dock_open'

function loadOpen(): boolean {
  try {
    return localStorage.getItem(OPEN_KEY) === '1'
  } catch {
    return false
  }
}

/**
 * Floating chat button + right-side sliding panel (epic_067 t878 behaviour). The page stays
 * usable while it is open, and the panel is kept mounted while closed so a running turn is
 * not interrupted. Mount once in the layout: `<ChatDock />`. Hidden on the /chat route.
 */
export default function ChatDock() {
  const { pathname } = useLocation()
  const [open, setOpenState] = useState(loadOpen)
  const chat = useChat()

  const setOpen = (v: boolean) => {
    setOpenState(v)
    try {
      localStorage.setItem(OPEN_KEY, v ? '1' : '0')
    } catch {
      /* ignore */
    }
  }

  if (pathname.startsWith('/chat')) return null
  return (
    <>
      {!open && (
        <button
          type="button"
          onClick={() => setOpen(true)}
          title="Chat"
          className="fixed bottom-5 right-5 z-40 flex h-12 w-12 items-center justify-center rounded-full bg-cq-blue text-white shadow-lg hover:opacity-90"
        >
          <MessageSquare size={22} />
          {chat.busy && <span className="absolute right-0 top-0 h-3 w-3 animate-pulse rounded-full bg-cq-gold" />}
        </button>
      )}
      <aside
        aria-hidden={!open}
        className={`fixed bottom-0 right-0 top-0 z-40 w-[min(100vw,440px)] border-l border-cq-grid bg-cq-bg shadow-2xl transition-transform duration-200 ${
          open ? 'translate-x-0' : 'pointer-events-none translate-x-full'
        }`}
      >
        <ChatPanel chat={chat} variant="dock" onClose={() => setOpen(false)} onPopout={openChatPopout} />
      </aside>
    </>
  )
}
