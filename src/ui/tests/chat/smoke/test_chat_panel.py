"""AppTest smoke for the chat panel (fake stream, no real CLI)."""

from __future__ import annotations

import pytest
from streamlit.testing.v1 import AppTest

pytestmark = pytest.mark.smoke

SCRIPT = """
import sys
sys.path.insert(0, "src")
from ui.components.chat_panel import render_chat_panel
render_chat_panel()
"""


def _fake(message, session_id=None):
    yield ("session", {"id": "S9"})
    yield ("delta", {"text": "pong"})
    yield ("done", {"session_id": "S9", "duration_ms": 10, "cost_usd": 0})


def test_panel_renders_and_new_conversation_resets(monkeypatch) -> None:
    from ui.chat import runner
    monkeypatch.setattr(runner, "stream_chat", _fake)
    at = AppTest.from_string(SCRIPT, default_timeout=10).run()
    assert not at.exception
    assert not any(b.label == "Új beszélgetés" for b in at.button)  # closed by default
    next(b for b in at.button if b.key == "chat_toggle").click().run()
    assert at.session_state["chat_open"] is True
    assert any(b.label == "Új beszélgetés" for b in at.button)
    at.chat_input[0].set_value("ping").run(timeout=10)
    assert not at.exception
    state = at.session_state["chat_state"]
    import time
    for _ in range(50):
        if not state.running:
            break
        time.sleep(0.1)
    assert state.session_id == "S9"
    at.run()
    assert any("pong" in m.value for m in at.markdown)
    next(b for b in at.button if b.label == "Új beszélgetés").click().run()
    assert at.session_state["chat_state"].messages == []


def test_toggle_closes_panel_and_keeps_turn_state(monkeypatch) -> None:
    from ui.chat import runner
    monkeypatch.setattr(runner, "stream_chat", _fake)
    at = AppTest.from_string(SCRIPT, default_timeout=10).run()
    next(b for b in at.button if b.key == "chat_toggle").click().run()
    at.chat_input[0].set_value("ping").run(timeout=10)
    state = at.session_state["chat_state"]
    assert [m.role for m in state.snapshot()] == ["user", "assistant"]  # shown immediately
    next(b for b in at.button if b.key == "chat_close").click().run()
    assert at.session_state["chat_open"] is False
    assert at.session_state["chat_state"] is state  # closing never replaces the state
    import time
    for _ in range(50):
        if not state.running:
            break
        time.sleep(0.1)
    assert state.session_id == "S9" and state.snapshot()[-1].text == "pong"
