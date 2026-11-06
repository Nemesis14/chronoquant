/** Open the chat in its own browser window (same origin: shared localStorage + BroadcastChannel). */
export function openChatPopout() {
  const w = window.open('/chat?popout=1', 'cq-chat', 'popup=yes,width=520,height=780')
  w?.focus()
}
