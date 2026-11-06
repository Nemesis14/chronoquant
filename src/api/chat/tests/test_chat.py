"""Chat runner + /api/chat SSE router tests (fake claude binary, never a real model)."""

from __future__ import annotations

import json
import os
import sys
import threading
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from api.chat import runner
from api.main import create_app
from api.routers import chat as chat_router

FAKE = str(Path(__file__).with_name("fake_claude.py"))


@pytest.fixture
def fake_claude(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Route the runner to the fake script; returns the pid-file path."""
    pid_file = tmp_path / "claude.pid"
    monkeypatch.setenv("FAKE_CLAUDE_PID_FILE", str(pid_file))
    monkeypatch.setattr(runner, "resolve_claude_bin", lambda: sys.executable)
    monkeypatch.setattr(runner, "build_command", lambda *a, **k: [sys.executable, FAKE])
    return pid_file


def parse_sse(text: str) -> list[tuple[str, dict]]:
    out = []
    for block in text.strip().split("\n\n"):
        lines = block.split("\n")
        out.append((lines[0].removeprefix("event: "), json.loads(lines[1].removeprefix("data: "))))
    return out


# --- command / env -----------------------------------------------------------------------------

def test_build_command_policy() -> None:
    cmd = runner.build_command("claude", model="sonnet", resume="abc", system_prompt="P")
    arg = lambda flag: cmd[cmd.index(flag) + 1]  # noqa: E731
    assert arg("--tools") == "Read,WebSearch,WebFetch"
    assert arg("--allowedTools") == "Read,WebSearch,WebFetch,mcp__chronoquant"
    assert set(arg("--disallowedTools").split(",")) == {"Edit", "Write", "NotebookEdit", "Bash"}
    assert arg("--setting-sources") == ""
    assert arg("--permission-mode") == "dontAsk"
    assert "--strict-mcp-config" in cmd
    assert json.loads(arg("--mcp-config"))["mcpServers"]["chronoquant"]["url"].endswith("/mcp/chronoquant")
    assert arg("--resume") == "abc"
    assert arg("--append-system-prompt") == "P"


def test_build_command_new_session_has_no_resume() -> None:
    assert "--resume" not in runner.build_command("claude", model="sonnet")


def test_build_env_strips_api_keys() -> None:
    env = runner.build_env({"ANTHROPIC_API_KEY": "k", "ANTHROPIC_AUTH_TOKEN": "t", "PATH": "p"})
    assert env == {"PATH": "p"}


def test_persona_prompt_loaded_and_mentions_mcp() -> None:
    text = runner.load_persona_prompt()
    assert "mcp__chronoquant" in text
    assert "Streamlit" not in text


def test_map_cli_event_text_and_result() -> None:
    state: dict = {}
    assert runner.map_cli_event({"type": "system", "subtype": "init", "session_id": "s"}, state) == [
        ("session", {"id": "s"})]
    ev = {"type": "stream_event", "event": {"type": "content_block_delta", "index": 0,
                                            "delta": {"type": "text_delta", "text": "hi"}}}
    assert runner.map_cli_event(ev, state) == [("delta", {"text": "hi"})]
    out = runner.map_cli_event({"type": "result", "session_id": "s", "duration_ms": 5, "total_cost_usd": 0.1}, state)
    assert out[0][0] == "done" and state["finished"]


# --- stream_chat with the fake binary ----------------------------------------------------------

def test_stream_chat_normal(fake_claude: Path) -> None:
    events = list(runner.stream_chat("question"))
    names = [e for e, _ in events]
    assert names[0] == "session"
    assert names[-1] == "done"
    text = "".join(d["text"] for e, d in events if e == "delta")
    assert text == "Hello **world**"
    tools = [d for e, d in events if e == "tool"]
    assert [t["status"] for t in tools] == ["start", "done"]
    assert tools[0]["name"] == "mcp__chronoquant__get_status"


def test_stream_chat_env_not_leaked(fake_claude: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "secret")
    events = list(runner.stream_chat("env"))
    assert "leaked=[]" in "".join(d["text"] for e, d in events if e == "delta")


def test_stream_chat_cli_exit_error(fake_claude: Path) -> None:
    events = list(runner.stream_chat("crash"))
    assert events[-1][0] == "error" and events[-1][1]["code"] == "cli_exit"
    assert "boom" in events[-1][1]["message"]


def test_stream_chat_binary_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom() -> str:
        raise runner.ChatBinaryNotFound("nope")

    monkeypatch.setattr(runner, "resolve_claude_bin", boom)
    assert list(runner.stream_chat("x")) == [("error", {"message": "nope", "code": "binary_not_found"})]


def _pid_alive(pid: int) -> bool:
    if os.name == "nt":
        import subprocess  # noqa: PLC0415

        out = subprocess.run(["tasklist", "/FI", f"PID eq {pid}"], capture_output=True, text=True, check=False)
        return str(pid) in out.stdout
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _wait_dead(pid: int, timeout: float = 8.0) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if not _pid_alive(pid):
            return True
        time.sleep(0.1)
    return False


def test_cancel_event_kills_process(fake_claude: Path) -> None:
    cancel = threading.Event()
    gen = runner.stream_chat("hang", None, cancel)
    assert next(gen)[0] == "session"
    pid = int(fake_claude.read_text())
    assert _pid_alive(pid)
    cancel.set()
    list(gen)  # drains: generator ends after the cancel
    assert _wait_dead(pid)


# --- router ------------------------------------------------------------------------------------

@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    return TestClient(create_app(frontend_dist=tmp_path / "no_dist"))


def test_chat_endpoint_streams_sse(fake_claude: Path, client: TestClient) -> None:
    with client.stream("POST", "/api/chat", json={"message": "hello", "session_id": None}) as resp:
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/event-stream")
        body = "".join(resp.iter_text())
    events = parse_sse(body)
    assert [e for e, _ in events][0] == "session"
    assert events[0][1] == {"id": "sess-fake-1"}
    assert events[-1][0] == "done"
    assert chat_router._slots._value == chat_router.MAX_CONCURRENT  # slot released  # noqa: SLF001


def test_chat_endpoint_validation(client: TestClient) -> None:
    assert client.post("/api/chat", json={"message": ""}).status_code == 422
    assert client.post("/api/chat", json={"message": "x" * 8001}).status_code == 422


def test_chat_endpoint_binary_missing_is_503_json(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    def boom() -> str:
        raise runner.ChatBinaryNotFound("no claude")

    monkeypatch.setattr(runner, "resolve_claude_bin", boom)
    resp = client.post("/api/chat", json={"message": "hi"})
    assert resp.status_code == 503
    assert resp.json() == {"detail": "no claude", "code": "binary_not_found"}


def test_chat_endpoint_busy_429(fake_claude: Path, client: TestClient) -> None:
    taken = [chat_router._slots.acquire(blocking=False) for _ in range(chat_router.MAX_CONCURRENT)]  # noqa: SLF001
    try:
        resp = client.post("/api/chat", json={"message": "hi"})
        assert resp.status_code == 429 and resp.json()["code"] == "busy"
    finally:
        for ok in taken:
            if ok:
                chat_router._slots.release()  # noqa: SLF001


def test_chat_config_endpoint(fake_claude: Path, client: TestClient) -> None:
    data = client.get("/api/chat/config").json()
    assert "mcp__chronoquant" in data["tools"]
    assert data["binary_ok"] is True


def test_async_consumer_close_kills_process(fake_claude: Path) -> None:
    """Client disconnect == the async generator is closed -> the claude process tree dies."""
    import asyncio  # noqa: PLC0415

    async def scenario() -> int:
        gen = runner.stream_chat_async("hang", None)
        first = await gen.__anext__()
        assert first[0] == "session"
        pid = int(fake_claude.read_text())
        await gen.aclose()
        return pid

    pid = asyncio.run(scenario())
    assert _wait_dead(pid)
