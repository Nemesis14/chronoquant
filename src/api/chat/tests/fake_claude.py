"""Fake `claude -p` for tests: reads stdin, prints stream-json lines. Never calls a real model.

Scenario comes from the stdin message: "hang" -> init then sleep; "crash" -> exit without result;
"env" -> reports whether API-key env vars leaked; anything else -> normal answer with a tool call.
"""

from __future__ import annotations

import json
import os
import sys
import time


def emit(obj: dict) -> None:
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def delta(text: str, idx: int = 0) -> dict:
    return {"type": "stream_event", "event": {
        "type": "content_block_delta", "index": idx, "delta": {"type": "text_delta", "text": text}}}


def main() -> None:
    message = sys.stdin.read()
    pid_file = os.environ.get("FAKE_CLAUDE_PID_FILE")
    if pid_file:
        with open(pid_file, "w", encoding="utf-8") as fh:
            fh.write(str(os.getpid()))
    emit({"type": "system", "subtype": "init", "session_id": "sess-fake-1"})
    if "hang" in message:
        emit(delta("start "))
        time.sleep(60)
        return
    if "crash" in message:
        sys.stderr.write("boom\n")
        return
    if "env" in message:
        leaked = [k for k in ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN") if k in os.environ]
        emit(delta(f"leaked={leaked}"))
    else:
        emit({"type": "stream_event", "event": {"type": "content_block_start", "index": 1, "content_block": {
            "type": "tool_use", "id": "toolu_1", "name": "mcp__chronoquant__get_status", "input": {}}}})
        emit({"type": "stream_event", "event": {"type": "content_block_stop", "index": 1}})
        emit({"type": "user", "message": {"content": [
            {"type": "tool_result", "tool_use_id": "toolu_1", "content": "ok"}]}})
        emit(delta("Hello "))
        emit(delta("**world**"))
    emit({"type": "result", "session_id": "sess-fake-1", "duration_ms": 12, "total_cost_usd": 0.0})


if __name__ == "__main__":
    main()
