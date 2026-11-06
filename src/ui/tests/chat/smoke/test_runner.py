"""Smoke tests for ui.chat.runner: event mapping, command building, env filtering.

The real claude CLI is never invoked; subprocess spawning is replaced by a fake.
"""

from __future__ import annotations

import io
import json

import pytest

from ui.chat import runner

pytestmark = pytest.mark.smoke

CFG = {
    "model"               : "sonnet",
    "timeout_s"           : 5,
    "persona_prompt_path" : "does/not/exist.md",
    "allowed_bash"        : [
        "uv run python research/tv_report/*",
        "uv run python src/data_handling/07_query_readonly.py*",
    ],
}


# --- command building ---

def test_build_command_flags_and_allowlist() -> None:
    cmd = runner.build_command("claude", model="sonnet", resume="abc", system_prompt="P", chat_cfg=CFG)
    assert cmd[:2] == ["claude", "-p"]
    assert cmd[cmd.index("--output-format") + 1] == "stream-json"
    assert "--verbose" in cmd and "--include-partial-messages" in cmd
    assert cmd[cmd.index("--resume") + 1] == "abc"
    assert cmd[cmd.index("--permission-mode") + 1] == "dontAsk"
    assert cmd[cmd.index("--append-system-prompt") + 1] == "P"
    assert cmd[cmd.index("--tools") + 1] == "Read,WebSearch,WebFetch,Bash"


def test_project_settings_not_loaded() -> None:
    """Project/user settings allow bare Bash/Edit/Write; they must not apply to the chat."""
    cmd = runner.build_command("claude", model="m", chat_cfg=CFG)
    assert cmd[cmd.index("--setting-sources") + 1] == ""
    assert "--strict-mcp-config" in cmd


def test_no_edit_or_write_allowed() -> None:
    cmd     = runner.build_command("claude", model="m", chat_cfg=CFG)
    tools   = cmd[cmd.index("--tools") + 1].split(",")
    allowed = cmd[cmd.index("--allowedTools") + 1].split(",")
    for forbidden in ("Edit", "Write", "NotebookEdit"):
        assert forbidden not in tools
        assert forbidden not in allowed
    assert "Bash(uv run python research/tv_report/*)" in allowed
    assert "Bash(uv run python src/data_handling/07_query_readonly.py*)" in allowed
    assert "Bash" not in allowed  # no bare, unrestricted Bash


def test_no_resume_when_new_session() -> None:
    assert "--resume" not in runner.build_command("claude", model="m", chat_cfg=CFG)


def test_env_strips_api_credentials() -> None:
    base = {"ANTHROPIC_API_KEY": "k", "ANTHROPIC_AUTH_TOKEN": "t", "PATH": "x"}
    env  = runner.build_env(base)
    assert env == {"PATH": "x"}
    assert "ANTHROPIC_API_KEY" in base  # input untouched


def test_persona_prompt_fallback_when_missing() -> None:
    assert runner.load_persona_prompt(CFG) == runner._FALLBACK_PROMPT


def test_persona_prompt_read_from_file(tmp_path) -> None:
    p = tmp_path / "persona.md"
    p.write_text("  hello persona \n", encoding="utf-8")
    assert runner.load_persona_prompt({**CFG, "persona_prompt_path": str(p)}) == "hello persona"


def test_resolve_claude_bin_env_missing(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("CLAUDE_BIN", str(tmp_path / "nope.exe"))
    with pytest.raises(runner.ChatBinaryNotFound):
        runner.resolve_claude_bin()


def test_resolve_claude_bin_env_ok(monkeypatch, tmp_path) -> None:
    f = tmp_path / "claude.exe"
    f.write_text("x")
    monkeypatch.setenv("CLAUDE_BIN", str(f))
    assert runner.resolve_claude_bin() == str(f)


def test_resolve_claude_bin_not_found(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("CLAUDE_BIN", raising=False)
    monkeypatch.setattr(runner.shutil, "which", lambda _n: None)
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    with pytest.raises(runner.ChatBinaryNotFound):
        runner.resolve_claude_bin()


# --- event mapping ---

def _map(objs: list[dict]) -> list[tuple[str, dict]]:
    state: dict = {}
    out: list[tuple[str, dict]] = []
    for o in objs:
        out += runner.map_cli_event(o, state)
    return out


def test_mapping_full_turn() -> None:
    events = _map([
        {"type": "system", "subtype": "init", "session_id": "S1"},
        {"type": "stream_event", "event": {"type": "content_block_delta",
                                           "delta": {"type": "text_delta", "text": "Hi"}}},
        {"type": "stream_event", "event": {"type": "content_block_start",
                                           "content_block": {"type": "tool_use", "id": "t1", "name": "Bash"}}},
        {"type": "assistant", "message": {"content": [
            {"type": "text", "text": "Hi"},
            {"type": "tool_use", "id": "t1", "name": "Bash"}]}},
        {"type": "user", "message": {"content": [
            {"type": "tool_result", "tool_use_id": "t1", "is_error": False}]}},
        {"type": "result", "session_id": "S1", "duration_ms": 10, "total_cost_usd": 0.1},
    ])
    assert events == [
        ("session", {"id": "S1"}),
        ("delta", {"text": "Hi"}),
        ("tool", {"name": "Bash", "status": "start", "id": "t1"}),
        ("tool", {"name": "Bash", "status": "done", "id": "t1"}),
        ("done", {"session_id": "S1", "duration_ms": 10, "cost_usd": 0.1}),
    ]


def test_mapping_tool_error_and_assistant_text_fallback() -> None:
    events = _map([
        {"type": "assistant", "message": {"content": [
            {"type": "text", "text": "full text"},
            {"type": "tool_use", "id": "t2", "name": "Read"}]}},
        {"type": "user", "message": {"content": [
            {"type": "tool_result", "tool_use_id": "t2", "is_error": True}]}},
    ])
    assert ("delta", {"text": "full text"}) in events
    assert ("tool", {"name": "Read", "status": "start", "id": "t2"}) in events
    assert ("tool", {"name": "Read", "status": "error", "id": "t2"}) in events


def test_mapping_error_result() -> None:
    events = _map([{"type": "result", "is_error": True, "result": "Not logged in"}])
    assert events[0][0] == "error"
    assert "logged in" in events[0][1]["message"]


def test_session_sent_once() -> None:
    init = {"type": "system", "subtype": "init", "session_id": "S"}
    assert len(_map([init, init])) == 1


# --- stream_chat with a fake process ---

class _FakeProc:
    def __init__(self, lines: list[dict], hang: bool = False) -> None:
        data        = "".join(json.dumps(o) + "\n" for o in lines).encode()
        self.stdout = _HangingStream(data) if hang else io.BytesIO(data)
        self.stderr = io.BytesIO(b"")
        self.stdin  = io.BytesIO()
        self.pid    = 1
        self.returncode: int | None = 0

    def poll(self) -> int | None:
        return 0

    def wait(self) -> int:
        return 0

    def kill(self) -> None:
        pass


class _HangingStream(io.BytesIO):
    def __iter__(self):
        yield from super().__iter__()
        import time
        time.sleep(3)


def _patch(monkeypatch, proc: _FakeProc, timeout: int = 5) -> None:
    monkeypatch.setattr(runner.utils, "load_chat_config", lambda: {**CFG, "timeout_s": timeout})
    monkeypatch.setattr(runner, "resolve_claude_bin", lambda: "claude")
    monkeypatch.setattr(runner, "_spawn", lambda *_a, **_k: proc)


def test_stream_chat_happy_path(monkeypatch) -> None:
    proc = _FakeProc([
        {"type": "system", "subtype": "init", "session_id": "S"},
        {"type": "stream_event", "event": {"type": "content_block_delta",
                                           "delta": {"type": "text_delta", "text": "ok"}}},
        {"type": "result", "session_id": "S"},
    ])
    _patch(monkeypatch, proc)
    kinds = [e for e, _ in runner.stream_chat("hello")]
    assert kinds == ["session", "delta", "done"]
    assert proc.stdin.closed


def test_stream_chat_eof_without_result(monkeypatch) -> None:
    proc = _FakeProc([{"type": "system", "subtype": "init", "session_id": "S"}])
    proc.returncode = 1
    _patch(monkeypatch, proc)
    events = list(runner.stream_chat("hello"))
    assert events[-1][0] == "error" and events[-1][1]["code"] == "cli_exit"


def test_stream_chat_timeout(monkeypatch) -> None:
    proc = _FakeProc([], hang=True)
    _patch(monkeypatch, proc, timeout=0)
    events = list(runner.stream_chat("hello"))
    assert events[-1][1]["code"] == "timeout"


def test_stream_chat_binary_missing(monkeypatch) -> None:
    def _boom() -> str:
        raise runner.ChatBinaryNotFound("nope")

    monkeypatch.setattr(runner.utils, "load_chat_config", lambda: CFG)
    monkeypatch.setattr(runner, "resolve_claude_bin", _boom)
    events = list(runner.stream_chat("hello"))
    assert events == [("error", {"message": "nope", "code": "binary_not_found"})]


# %% thinking + tool input (t878)

def _se(event: dict) -> dict:
    return {"type": "stream_event", "event": event}


def test_mapping_thinking_events() -> None:
    state: dict = {}
    out = runner.map_cli_event(_se({"type": "content_block_start", "index": 0,
                                    "content_block": {"type": "thinking", "thinking": ""}}), state)
    out += runner.map_cli_event(_se({"type": "content_block_delta", "index": 0,
                                     "delta": {"type": "thinking_delta", "thinking": "hm",
                                               "estimated_tokens": 42}}), state)
    assert out == [("thinking", {"text": "", "tokens": 0}), ("thinking", {"text": "hm", "tokens": 42})]


def test_mapping_streamed_tool_input_emitted_on_block_stop() -> None:
    state: dict = {}
    start = runner.map_cli_event(_se({"type": "content_block_start", "index": 1, "content_block": {
        "type": "tool_use", "id": "T1", "name": "Bash", "input": {}}}), state)
    assert start == [("tool", {"name": "Bash", "status": "start", "id": "T1"})]
    for part in ('{"command": "uv run ', 'python tv_report.py"}'):
        assert runner.map_cli_event(_se({"type": "content_block_delta", "index": 1, "delta": {
            "type": "input_json_delta", "partial_json": part}}), state) == []
    stop = runner.map_cli_event(_se({"type": "content_block_stop", "index": 1}), state)
    assert stop == [("tool", {"name": "Bash", "status": "start", "id": "T1",
                              "input": "uv run python tv_report.py"})]
    # the full assistant message repeats the input -> must not be emitted twice
    again = runner.map_cli_event({"type": "assistant", "message": {"content": [{
        "type": "tool_use", "id": "T1", "name": "Bash", "input": {"command": "uv run python tv_report.py"}}]}},
        state)
    assert again == []


def test_mapping_tool_input_from_assistant_message_only() -> None:
    out = runner.map_cli_event({"type": "assistant", "message": {"content": [{
        "type": "tool_use", "id": "T2", "name": "Read", "input": {"file_path": "src/ui/main.py"}}]}}, {})
    assert out == [("tool", {"name": "Read", "status": "start", "id": "T2", "input": "src/ui/main.py"})]


def test_summarize_tool_input_truncates_and_handles_junk() -> None:
    assert runner.summarize_tool_input("Bash", None) == ""
    assert runner.summarize_tool_input("X", {}) == ""
    assert runner.summarize_tool_input("WebSearch", {"query": "sol  usdt"}) == "sol usdt"
    long = runner.summarize_tool_input("Bash", {"command": "x" * 1000})
    assert len(long) <= 403 and long.endswith("...")


def test_command_enables_thinking_settings() -> None:
    cmd = runner.build_command("claude", model="sonnet", chat_cfg={"allowed_bash": []})
    assert "--settings" in cmd
    assert "showThinkingSummaries" in cmd[cmd.index("--settings") + 1]
