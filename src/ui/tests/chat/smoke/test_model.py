"""Smoke tests for ui.chat.model (Streamlit-free chat state) + AppTest for the panel.

The real claude CLI is never invoked; a fake stream function is injected.
"""

from __future__ import annotations

import pytest

from ui.chat import model

pytestmark = pytest.mark.smoke


def _fake(events):
    calls: list[tuple[str, str | None]] = []

    def fn(message, session_id=None):
        calls.append((message, session_id))
        yield from events

    fn.calls = calls  # type: ignore[attr-defined]
    return fn


OK = [
    ("session", {"id": "S1"}),
    ("tool", {"name": "Bash", "status": "start", "id": "t1"}),
    ("tool", {"name": "Bash", "status": "done", "id": "t1"}),
    ("delta", {"text": "Hel"}),
    ("delta", {"text": "lo"}),
    ("done", {"session_id": "S1", "duration_ms": 1500, "cost_usd": 0.01}),
]


def test_turn_builds_messages_and_keeps_session() -> None:
    st = model.ChatState()
    fn = _fake(OK)
    model.start_turn(st, "  hi ", fn, background=False)
    msgs = st.snapshot()
    assert [m.role for m in msgs] == ["user", "assistant"]
    assert msgs[0].text == "hi"
    assert msgs[1].text == "Hello"
    assert msgs[1].tools[0].status == "done" and len(msgs[1].tools) == 1
    assert not msgs[1].running and msgs[1].duration_ms == 1500
    assert st.session_id == "S1" and not st.running


def test_second_turn_resumes_session() -> None:
    st = model.ChatState()
    fn = _fake(OK)
    model.start_turn(st, "a", fn, background=False)
    model.start_turn(st, "b", fn, background=False)
    assert fn.calls == [("a", None), ("b", "S1")]


@pytest.mark.parametrize("code", ["binary_not_found", "timeout", "cli_error", "cli_exit",
                                  "spawn_failed"])
def test_error_codes_are_readable(code: str) -> None:
    st = model.ChatState()
    model.start_turn(st, "x", _fake([("error", {"message": "detail", "code": code})]),
                     background=False)
    err = st.snapshot()[1].error
    assert err and "detail" in err and model.ERROR_TEXTS[code] in err
    assert not st.running


def test_dangling_tool_closed_and_exception_handled() -> None:
    def boom(message, session_id=None):
        yield ("tool", {"name": "Bash", "status": "start", "id": "t"})
        raise RuntimeError("kaboom")

    st = model.ChatState()
    model.start_turn(st, "x", boom, background=False)
    m = st.snapshot()[1]
    assert m.error and "kaboom" in m.error and m.tools[0].status == "error" and not m.running


def test_blank_and_concurrent_ignored() -> None:
    st = model.ChatState()
    assert model.start_turn(st, "   ", _fake(OK), background=False) is None
    assert st.messages == []
    st.messages.append(model.ChatMessage("assistant", running=True))
    model.start_turn(st, "x", _fake(OK), background=False)
    assert len(st.messages) == 1


def test_generator_closed_on_cancel() -> None:
    closed = []

    def gen(message, session_id=None):
        try:
            yield ("delta", {"text": "a"})
            yield ("delta", {"text": "b"})
        finally:
            closed.append(True)

    st = model.ChatState()
    st.cancelled = True
    model.start_turn(st, "x", gen, background=False)
    assert closed == [True]
    assert not st.running


def test_background_thread_finishes() -> None:
    st = model.ChatState()
    t = model.start_turn(st, "x", _fake(OK))
    assert t is not None
    t.join(5)
    assert not st.running and st.snapshot()[1].text == "Hello"


def test_apply_event_unknown_tool_label() -> None:
    assert "Foo" in model.tool_label("Foo")
    assert "tv_report" in model.tool_label("Bash")


def test_thinking_and_tool_input_fold_into_message() -> None:
    from ui.chat.model import ChatMessage, ChatState, apply_event
    state, msg = ChatState(), ChatMessage("assistant", running=True)
    apply_event(state, msg, "thinking", {"text": "", "tokens": 50})
    apply_event(state, msg, "thinking", {"text": "abc", "tokens": 90})
    assert msg.thinking_seen and msg.thinking == "abc" and msg.thinking_tokens == 90
    assert not msg.thinking_done
    apply_event(state, msg, "tool", {"id": "t", "name": "Bash", "status": "start"})
    assert msg.thinking_done and msg.tools[0].input == ""
    apply_event(state, msg, "tool", {"id": "t", "name": "Bash", "status": "start", "input": "ls"})
    apply_event(state, msg, "tool", {"id": "t", "name": "Bash", "status": "done"})
    assert msg.tools[0].input == "ls" and msg.tools[0].status == "done"
    state.messages.append(msg)
    snap = state.snapshot()[0]
    assert snap.thinking == "abc" and snap.tools[0].input == "ls" and snap.thinking_seen
