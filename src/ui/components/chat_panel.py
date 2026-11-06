from __future__ import annotations

import streamlit as st

from ui.chat.model import ChatMessage, ChatState, cancel, start_turn
from ui.components.formatting import scaled

_STATE_KEY = "chat_state"
_OPEN_KEY = "chat_open"
_POLL_INTERVAL = "0.5s"

# Panel width, scaled with the page (formatting.UI_ZOOM).
PANEL_WIDTH_PX = scaled(500)

# Static layout CSS for the floating button and the right-hand panel. Injected once from
# main.py (the keyed containers below get the `st-key-<key>` class from Streamlit).
CHAT_DOCK_CSS = f"""
.st-key-chat_fab {{
    position: fixed; right: 20px; bottom: 20px; z-index: 1001; width: auto !important;
}}
.st-key-chat_fab button {{
    border-radius: 999px; padding: 0.55rem 1.1rem;
    box-shadow: 0 4px 14px rgba(0, 0, 0, 0.55);
}}
.st-key-chat_dock {{
    position: fixed; top: 0; right: 0; bottom: 0; z-index: 1000;
    width: {PANEL_WIDTH_PX}px; max-width: 100vw;
    background: #111418; border-left: 1px solid #2a2f36;
    padding: 1rem 1rem 1rem 1rem; gap: 0.6rem; overflow: hidden;
    box-shadow: -8px 0 24px rgba(0, 0, 0, 0.45);
}}
.st-key-chat_dock, .st-key-chat_dock h3, .st-key-chat_dock p, .st-key-chat_dock li,
.st-key-chat_dock [data-testid="stCaptionContainer"] {{ color: #eaecef; }}
.st-key-chat_dock > [data-testid="stLayoutWrapper"]:has(> .st-key-chat_history) {{
    flex: 1 1 0; min-height: 0; display: flex;
}}
.st-key-chat_dock .st-key-chat_history {{
    flex: 1 1 0; min-height: 0; overflow-y: auto;
    display: flex; flex-direction: column-reverse; justify-content: flex-start;
}}
"""

# Added only while the panel is open: reserve room so the page content is not covered.
_OPEN_PAGE_CSS = f"""
<style>
@media (min-width: 1100px) {{
    [data-testid="stMainBlockContainer"], .block-container {{
        padding-right: {PANEL_WIDTH_PX + 24}px !important;
    }}
}}
</style>
"""


def _get_state() -> ChatState:
    state = st.session_state.get(_STATE_KEY)
    if not isinstance(state, ChatState):
        state = ChatState()
        st.session_state[_STATE_KEY] = state
    return state


def is_open() -> bool:
    return bool(st.session_state.get(_OPEN_KEY, False))


def toggle_open() -> None:
    """Button callback: open/close the panel. Never touches the running turn (it lives in
    a worker thread owned by the ChatState, independent of any rerun)."""
    st.session_state[_OPEN_KEY] = not is_open()


def new_conversation() -> None:
    cancel(_get_state())
    st.session_state[_STATE_KEY] = ChatState()


def _render_thinking(msg: ChatMessage) -> None:
    active = msg.running and not msg.thinking_done
    tokens = f" (~{msg.thinking_tokens} token)" if msg.thinking_tokens else ""
    label = f"Gondolkodik…{tokens}" if active else f"Gondolatmenet{tokens}"
    with st.expander(label, expanded=active, icon=":material/psychology:"):
        if msg.thinking.strip():
            st.caption(msg.thinking)
        else:
            st.caption("A modell gondolatmenete nem érhető el szövegként, csak a ráfordított idő látszik.")


def _render_tool(tool) -> None:
    icon = {"start": "⏳", "done": "✅", "error": "⚠️"}.get(tool.status, "")
    st.caption(f"{icon} **{tool.name or 'tool'}** - {tool.label}{'…' if tool.status == 'start' else ''}")
    if tool.input:
        st.code(tool.input, language="bash" if tool.name == "Bash" else None, wrap_lines=True)


def _render_message(msg: ChatMessage) -> None:
    with st.chat_message(msg.role):
        if msg.role == "user":
            st.markdown(msg.text)
            return
        if msg.thinking_seen:
            _render_thinking(msg)
        for tool in msg.tools:
            _render_tool(tool)
        if msg.text:
            st.markdown(msg.text + (" ▌" if msg.running else ""))
        elif msg.running and not msg.tools and not msg.thinking_seen:
            st.caption("Dolgozik…")
        if msg.error:
            st.error(msg.error)
        elif not msg.running and msg.duration_ms is not None:
            st.caption(f"{msg.duration_ms / 1000:.1f} s")


def _render_history(messages: list[ChatMessage]) -> None:
    # The container is column-reversed (CSS), so it stays scrolled to the newest message;
    # hence the DOM order is newest-first.
    if not messages:
        st.caption("Kérdezz a piacról, a stratégiáról vagy a modellekről.")
    for msg in reversed(messages):
        _render_message(msg)


def _render_panel() -> None:
    state = _get_state()
    with st.container(key="chat_dock"):
        head, btn, close = st.columns([4, 4, 1.4], vertical_alignment="center")
        head.subheader("Chat")
        btn.button("Új beszélgetés", key="chat_new", width="stretch", on_click=new_conversation)
        close.button("", key="chat_close", icon=":material/close:", width="stretch",
                     help="Panel bezárása (a futó válasz nem szakad meg)", on_click=toggle_open)

        history_slot = st.container(border=True, key="chat_history")
        prompt = st.chat_input("Írj a chatnek…", key="chat_input", disabled=state.running)
        if prompt:
            start_turn(state, prompt)
        with history_slot:
            _render_history(state.snapshot())


# One fragment hosts both the floating button and the panel, so opening/closing/sending
# only reruns this fragment (the full page script is slow: Binance REST + plotly). The
# run_every poll picks up the streamed turn from the worker thread; the turn itself never
# depends on any rerun.
@st.fragment(run_every=_POLL_INTERVAL)
def render_chat_panel() -> None:
    state = _get_state()
    opened = is_open()

    if not opened:
        with st.container(key="chat_fab"):
            label = "Chat (válaszol…)" if state.running else "Chat"
            st.button(label, key="chat_toggle", icon=":material/forum:", type="primary",
                      on_click=toggle_open)

    if opened:
        st.markdown(_OPEN_PAGE_CSS, unsafe_allow_html=True)
        _render_panel()
