"""Streamlit-independent chat state and event -> UI-model translation (epic_066 / t874).

The runner (`ui.chat.runner.stream_chat`) is a blocking sync generator. Consuming it
inside the Streamlit script body would be interrupted by every fragment/app rerun
(the sidebar sync panel reruns every 2 s), which closes the generator and kills the
claude process. So each turn is consumed by a worker thread that mutates a
`ChatState`; the UI only polls snapshots of it.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

StreamFn = Callable[[str, str | None], Iterable[tuple[str, dict]]]

TOOL_LABELS = {
    "Bash"     : "Elemzés / lekérdezés fut (tv_report, SQL) - ez percekig is tarthat",
    "WebSearch": "Webes keresés",
    "WebFetch" : "Weboldal letöltése",
    "Read"     : "Fájl olvasása",
}

ERROR_TEXTS = {
    "binary_not_found": "A `claude` CLI nem található. Telepítsd, vagy állítsd be a CLAUDE_BIN "
                        "környezeti változót.",
    "timeout"         : "Időtúllépés - a válasz megszakadt. Próbáld egyszerűbb kérdéssel.",
    "cli_error"       : "A claude CLI hibát jelzett.",
    "cli_exit"        : "A claude CLI váratlanul leállt.",
    "spawn_failed"    : "A claude CLI nem indítható.",
}


def tool_label(name: str) -> str:
    return TOOL_LABELS.get(name, f"Eszköz: {name or 'ismeretlen'}")


def error_text(code: str, message: str) -> str:
    base = ERROR_TEXTS.get(code, "Ismeretlen hiba.")
    return f"{base}\n\n{message}" if message else base


@dataclass
class ToolStatus:
    id: str
    name: str
    status: str = "start"  # start | done | error
    input: str = ""        # short human summary of the call (command / file / query)

    @property
    def label(self) -> str:
        return tool_label(self.name)


@dataclass
class ChatMessage:
    role: str                       # user | assistant
    text: str = ""
    tools: list[ToolStatus] = field(default_factory=list)
    error: str | None = None
    running: bool = False
    duration_ms: int | None = None
    cost_usd: float | None = None
    thinking: str = ""
    thinking_tokens: int = 0
    thinking_seen: bool = False     # a thinking block occurred
    thinking_done: bool = False     # the model moved on to text / tools


@dataclass
class ChatState:
    messages: list[ChatMessage] = field(default_factory=list)
    session_id: str | None = None
    lock: threading.RLock = field(default_factory=threading.RLock)
    cancelled: bool = False

    @property
    def running(self) -> bool:
        with self.lock:
            return bool(self.messages) and self.messages[-1].running

    def snapshot(self) -> list[ChatMessage]:
        """Deep-enough copy so the UI can render without holding the lock."""
        with self.lock:
            return [
                ChatMessage(m.role, m.text,
                            [ToolStatus(t.id, t.name, t.status, t.input) for t in m.tools],
                            m.error, m.running, m.duration_ms, m.cost_usd,
                            m.thinking, m.thinking_tokens, m.thinking_seen, m.thinking_done)
                for m in self.messages
            ]


def apply_event(state: ChatState, msg: ChatMessage, event: str, data: dict) -> None:
    """Fold one runner event into the assistant message / session state."""
    with state.lock:
        if event == "session":
            state.session_id = data.get("id") or state.session_id
        elif event == "thinking":
            msg.thinking_seen = True
            msg.thinking += data.get("text", "") or ""
            msg.thinking_tokens = max(msg.thinking_tokens, int(data.get("tokens") or 0))
        elif event == "delta":
            msg.thinking_done = True
            msg.text += data.get("text", "")
        elif event == "tool":
            msg.thinking_done = True
            tid = str(data.get("id") or "")
            existing = next((t for t in msg.tools if t.id == tid), None)
            if existing is None:
                msg.tools.append(ToolStatus(tid, data.get("name", ""), data.get("status", "start"),
                                            data.get("input", "")))
            else:
                if data.get("input"):
                    existing.input = data["input"]
                existing.status = data.get("status", existing.status)
                existing.name = data.get("name") or existing.name
        elif event == "done":
            state.session_id = data.get("session_id") or state.session_id
            msg.duration_ms = data.get("duration_ms")
            msg.cost_usd = data.get("cost_usd")
            msg.running = False
        elif event == "error":
            msg.error = error_text(data.get("code", ""), data.get("message", ""))
            msg.running = False


def _run_turn(state: ChatState, msg: ChatMessage, message: str, stream_fn: StreamFn) -> None:
    session_id = state.session_id
    gen = stream_fn(message, session_id)
    try:
        for event, data in gen:
            if state.cancelled:
                break
            apply_event(state, msg, event, data)
    except Exception as exc:  # never leave the UI stuck in "running"
        logger.exception("Chat worker failed")
        apply_event(state, msg, "error", {"code": "cli_error", "message": str(exc)})
    finally:
        close = getattr(gen, "close", None)
        if close:
            close()  # kills the claude process tree if still alive
        with state.lock:
            msg.running = False
            for t in msg.tools:
                if t.status == "start":
                    t.status = "error" if msg.error else "done"


def start_turn(state: ChatState, message: str, stream_fn: StreamFn | None = None,
               *, background: bool = True) -> threading.Thread | None:
    """Append the user message + a running assistant placeholder and start streaming.

    Returns the worker thread (None if `background` is False, in which case the turn
    is run inline - used by tests). Ignored if a turn is already running or the
    message is blank.
    """
    message = message.strip()
    if not message or state.running:
        return None
    if stream_fn is None:
        from ui.chat.runner import stream_chat
        stream_fn = stream_chat
    with state.lock:
        state.messages.append(ChatMessage("user", message))
        reply = ChatMessage("assistant", running=True)
        state.messages.append(reply)
    if not background:
        _run_turn(state, reply, message, stream_fn)
        return None
    thread = threading.Thread(target=_run_turn, args=(state, reply, message, stream_fn),
                              daemon=True, name="chat-worker")
    thread.start()
    return thread


def cancel(state: ChatState) -> None:
    """Detach a (possibly running) turn: the worker stops at its next event."""
    state.cancelled = True
