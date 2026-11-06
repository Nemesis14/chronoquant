"""Headless `claude -p` runner for the Streamlit chat (epic_066).

Spawns the local, logged-in `claude` CLI (subscription, NOT an API key) with a
narrow tool allowlist and exposes the run as a synchronous generator of
`(event, data)` pairs, suitable for `st.write_stream`.

Events:
    session {id}
    delta   {text}
    thinking {text, tokens}   (text may be empty: the API often omits thinking text)
    tool    {name, status: start|done|error, id, input?}   (input: short human summary)
    done    {session_id, duration_ms, cost_usd}
    error   {message, code}

Policy: ANTHROPIC_API_KEY / ANTHROPIC_AUTH_TOKEN are stripped from the child env,
Edit/Write are never enabled, cwd is the repo root.
"""

from __future__ import annotations

import glob
import json
import logging
import os
import queue
import shutil
import subprocess
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import utils

logger = logging.getLogger(__name__)

# %% Constants

# Never passed to the child: otherwise claude would bill the API instead of the subscription.
STRIPPED_ENV = ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN")

BUILTIN_TOOLS = ("Read", "WebSearch", "WebFetch", "Bash")
DENIED_TOOLS  = ("Edit", "Write", "NotebookEdit")

_FALLBACK_PROMPT = (
    "You are the ChronoQuant market assistant. Answer in the user's language. "
    "You have read-only access; never modify files."
)
# Enables extended thinking in -p mode so the panel can show the model "working" live.
_THINKING_SETTINGS = '{"showThinkingSummaries":true,"alwaysThinkingEnabled":true}'
_TOOL_INPUT_MAX = 400
_MAX_ARG_CHARS = 24000  # Windows command line limit is ~32k

ChatEvent = tuple[str, dict]


class ChatError(Exception):
    """User-facing, readable runner error."""

    def __init__(self, message: str, code: str = "error") -> None:
        super().__init__(message)
        self.message = message
        self.code    = code


class ChatBinaryNotFound(ChatError):
    """The `claude` executable could not be located."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code="binary_not_found")


# %% Binary resolution

def _version_key(path: str) -> list[tuple[int, int]]:
    parts: list[tuple[int, int]] = []
    for tok in Path(path).parent.parent.parent.name.replace("-", ".").split("."):
        parts.append((0, int(tok)) if tok.isdigit() else (-1, 0))
    return parts


def resolve_claude_bin() -> str:
    """Resolve the claude executable: CLAUDE_BIN env -> PATH -> VS Code extension glob.

    Raises:
        ChatBinaryNotFound: with an actionable message if nothing is found.
    """
    env_bin = os.environ.get("CLAUDE_BIN")
    if env_bin:
        if Path(env_bin).is_file():
            return env_bin
        raise ChatBinaryNotFound(f"The file given in CLAUDE_BIN does not exist: {env_bin}")
    on_path = shutil.which("claude")
    if on_path:
        return on_path
    home    = Path(os.environ.get("USERPROFILE") or Path.home())
    pattern = str(home / ".vscode" / "extensions" / "anthropic.claude-code-*"
                  / "resources" / "native-binary" / "claude.exe")
    matches = glob.glob(pattern)
    if matches:
        return max(matches, key=_version_key)
    raise ChatBinaryNotFound(
        "The `claude` CLI was not found. Install it, add it to PATH, or set the "
        "CLAUDE_BIN environment variable to the full path of claude.exe."
    )


# %% Command + env

def load_persona_prompt(chat_cfg: dict | None = None) -> str:
    """Read the persona prompt file at call time (repo-relative path from chat config)."""
    chat_cfg = chat_cfg or utils.load_chat_config()
    path = Path(chat_cfg["persona_prompt_path"])
    if not path.is_absolute():
        path = Path(utils.repo_root()) / path
    try:
        text = path.read_text(encoding="utf-8").strip()
    except OSError:
        logger.warning("Persona prompt not readable (%s); using fallback prompt", path)
        return _FALLBACK_PROMPT
    if len(text) > _MAX_ARG_CHARS:
        logger.warning("Persona prompt is %d chars; may exceed the OS command line limit", len(text))
    return text or _FALLBACK_PROMPT


def allowed_tools(chat_cfg: dict | None = None) -> list[str]:
    """`--allowedTools` entries: Read/WebSearch/WebFetch plus narrow Bash(...) patterns."""
    chat_cfg = chat_cfg or utils.load_chat_config()
    bash     = [f"Bash({pat})" for pat in chat_cfg["allowed_bash"]]
    return ["Read", "WebSearch", "WebFetch", *bash]


def build_command(
    binary        : str,
    *,
    model         : str,
    resume        : str | None  = None,
    system_prompt : str         = "",
    chat_cfg      : dict | None = None,
) -> list[str]:
    """Build the `claude -p` argv. The user message is passed via stdin, not argv."""
    cmd = [
        binary, "-p",
        "--output-format", "stream-json",
        "--verbose",  # mandatory for stream-json in -p mode
        "--include-partial-messages",
        "--tools", ",".join(BUILTIN_TOOLS),
        "--allowedTools", ",".join(allowed_tools(chat_cfg)),
        "--disallowedTools", ",".join(DENIED_TOOLS),
        "--permission-mode", "dontAsk",
        "--disable-slash-commands",
        # No project/user/local settings: .claude/settings*.json allow bare Bash/Edit/Write and
        # run Stop/UserPromptSubmit hooks, which would defeat the narrow allowlist above.
        "--setting-sources", "",
        "--strict-mcp-config",
        "--model", model,
        "--settings", _THINKING_SETTINGS,
    ]
    if system_prompt:
        cmd += ["--append-system-prompt", system_prompt]
    if resume:
        cmd += ["--resume", resume]
    return cmd


def build_env(base: dict | None = None) -> dict:
    """Child environment with API credentials removed."""
    env = dict(os.environ if base is None else base)
    for key in STRIPPED_ENV:
        env.pop(key, None)
    return env


# %% stream-json -> events

def _friendly_error(text: str) -> str:
    low = text.lower()
    if any(k in low for k in ("not logged in", "/login", "authentication", "invalid api key", "oauth")):
        return ("The claude CLI is not logged in. Open a terminal, run `claude`, log in "
                "(/login) and retry. Detail: " + text[:300])
    return text[:1000] or "Unknown error from the claude CLI."


def summarize_tool_input(name: str, data: object) -> str:
    """One short line describing what a tool call does (command, file, query, url)."""
    if not isinstance(data, dict):
        return ""
    for key in ("command", "file_path", "query", "url", "pattern", "path"):
        val = data.get(key)
        if val:
            text = " ".join(str(val).split())
            return text if len(text) <= _TOOL_INPUT_MAX else text[:_TOOL_INPUT_MAX] + "..."
    if not data:
        return ""
    text = json.dumps(data, ensure_ascii=False)
    return text if len(text) <= _TOOL_INPUT_MAX else text[:_TOOL_INPUT_MAX] + "..."


def _tool_start(state: dict, tid: str | None, name: str, tool_input: object = None) -> ChatEvent | None:
    """Tool 'start' event; emitted once more when the (streamed) input becomes known."""
    summary = summarize_tool_input(name, tool_input)
    seen = state.setdefault("tool_started", set())
    sent = state.setdefault("tool_input_sent", set())
    if tid in seen and (not summary or tid in sent):
        return None
    seen.add(tid)
    data: dict = {"name": name, "status": "start", "id": tid}
    if summary:
        sent.add(tid)
        data["input"] = summary
    return "tool", data


def map_cli_event(obj: dict, state: dict) -> list[ChatEvent]:
    """Translate one claude stream-json object into 0..n chat events.

    `state` is the per-run mutable state (session sent, tool id -> name, streamed tool input
    buffers, delta seen, finished flag).
    """
    out: list[ChatEvent] = []
    typ = obj.get("type")

    if typ == "system" and obj.get("subtype") == "init":
        sid = obj.get("session_id")
        if sid and not state.get("session_sent"):
            state["session_sent"] = True
            state["session_id"]   = sid
            out.append(("session", {"id": sid}))

    elif typ == "stream_event":
        ev = obj.get("event") or {}
        et = ev.get("type")
        idx = ev.get("index")
        if et == "content_block_start":
            block = ev.get("content_block") or {}
            if block.get("type") == "tool_use":
                tid = block.get("id")
                state.setdefault("tool_names", {})[tid] = block.get("name", "")
                state.setdefault("tool_index", {})[idx] = tid
                state.setdefault("tool_json", {})[tid] = ""
                evt = _tool_start(state, tid, block.get("name", ""), block.get("input") or None)
                if evt:
                    out.append(evt)
            elif block.get("type") == "thinking":
                out.append(("thinking", {"text": block.get("thinking") or "", "tokens": 0}))
        elif et == "content_block_delta":
            delta = ev.get("delta") or {}
            dtype = delta.get("type")
            if dtype == "text_delta" and delta.get("text"):
                state["got_delta"] = True
                out.append(("delta", {"text": delta["text"]}))
            elif dtype == "thinking_delta":
                out.append(("thinking", {"text": delta.get("thinking") or "",
                                         "tokens": delta.get("estimated_tokens") or 0}))
            elif dtype == "input_json_delta":
                tid = state.get("tool_index", {}).get(idx)
                if tid is not None:
                    state["tool_json"][tid] += delta.get("partial_json") or ""
        elif et == "content_block_stop":
            tid = state.get("tool_index", {}).pop(idx, None)
            if tid is not None:
                raw = state.get("tool_json", {}).pop(tid, "")
                try:
                    parsed = json.loads(raw) if raw else {}
                except json.JSONDecodeError:
                    parsed = {}
                evt = _tool_start(state, tid, state.get("tool_names", {}).get(tid, ""), parsed)
                if evt:
                    out.append(evt)

    elif typ == "assistant":
        for block in (obj.get("message") or {}).get("content", []) or []:
            if block.get("type") == "tool_use":
                tid = block.get("id")
                state.setdefault("tool_names", {}).setdefault(tid, block.get("name", ""))
                evt = _tool_start(state, tid, block.get("name", ""), block.get("input"))
                if evt:
                    out.append(evt)
            elif block.get("type") == "text" and not state.get("got_delta") and block.get("text"):
                out.append(("delta", {"text": block["text"]}))
        state["got_delta"] = False

    elif typ == "user":
        for block in (obj.get("message") or {}).get("content", []) or []:
            if isinstance(block, dict) and block.get("type") == "tool_result":
                tid = block.get("tool_use_id")
                out.append(("tool", {
                    "name"  : state.get("tool_names", {}).get(tid, ""),
                    "status": "error" if block.get("is_error") else "done",
                    "id"    : tid,
                }))

    elif typ == "result":
        if obj.get("is_error"):
            out.append(("error", {"message": _friendly_error(str(obj.get("result") or "")),
                                  "code": "cli_error"}))
        else:
            out.append(("done", {
                "session_id" : obj.get("session_id") or state.get("session_id"),
                "duration_ms": obj.get("duration_ms"),
                "cost_usd"   : obj.get("total_cost_usd"),
            }))
        state["finished"] = True

    return out


# %% Process handling

def kill_tree(proc: subprocess.Popen) -> None:
    """Kill the process and all its children (Windows: taskkill /T)."""
    if proc.poll() is not None:
        return
    try:
        if os.name == "nt":
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                           capture_output=True, timeout=10, check=False,
                           creationflags=subprocess.CREATE_NO_WINDOW)
        else:
            proc.kill()
    except Exception:
        logger.exception("Process tree kill failed for pid %s", proc.pid)
    try:
        proc.kill()
    except Exception:
        logger.debug("proc.kill() after tree kill failed", exc_info=True)


def _spawn(cmd: list[str], env: dict, cwd: str) -> subprocess.Popen:
    kwargs: dict = {}
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
    return subprocess.Popen(
        cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env=env, cwd=cwd, **kwargs,
    )


_EOF = object()


def _pump_stdout(proc: subprocess.Popen, q: queue.Queue) -> None:
    try:
        assert proc.stdout is not None
        for raw in proc.stdout:
            q.put(raw)
    except Exception:
        logger.debug("stdout pump stopped", exc_info=True)
    finally:
        q.put(_EOF)


def _drain_stderr(proc: subprocess.Popen, buf: list[bytes]) -> None:
    try:
        assert proc.stderr is not None
        for line in proc.stderr:
            buf.append(line)
    except Exception:
        logger.debug("stderr drain stopped", exc_info=True)


# %% Public API

def stream_chat(message: str, session_id: str | None = None) -> Iterator[ChatEvent]:
    """Run one chat turn and yield `(event, data)` pairs.

    Args:
        message    : User message (sent to claude via stdin).
        session_id : Previous session id to `--resume`, or None for a new session.

    Yields:
        session / delta / tool / done / error events. Runtime failures (timeout, CLI
        error, unexpected exit, missing binary) are yielded as `error` events, not raised.
        Closing the generator early kills the whole claude process tree.
    """
    chat_cfg = utils.load_chat_config()
    try:
        binary = resolve_claude_bin()
    except ChatBinaryNotFound as exc:
        yield "error", {"message": exc.message, "code": exc.code}
        return

    cmd = build_command(binary, model=chat_cfg["model"], resume=session_id,
                        system_prompt=load_persona_prompt(chat_cfg), chat_cfg=chat_cfg)
    try:
        proc = _spawn(cmd, build_env(), utils.repo_root())
    except OSError as exc:
        logger.exception("Failed to start claude")
        yield "error", {"message": f"Could not start the claude CLI: {exc}", "code": "spawn_failed"}
        return

    timeout  = float(chat_cfg["timeout_s"])
    deadline = time.monotonic() + timeout
    state: dict = {}
    stderr_buf: list[bytes] = []
    q: queue.Queue = queue.Queue()
    threading.Thread(target=_pump_stdout, args=(proc, q), daemon=True).start()
    threading.Thread(target=_drain_stderr, args=(proc, stderr_buf), daemon=True).start()

    try:
        # --- send the message ---
        try:
            assert proc.stdin is not None
            proc.stdin.write(message.encode("utf-8"))
            proc.stdin.close()
        except OSError:
            logger.debug("stdin write failed (process may have exited)", exc_info=True)

        # --- read events ---
        while True:
            remaining = deadline - time.monotonic()
            try:
                if remaining <= 0:
                    raise queue.Empty
                raw = q.get(timeout=remaining)
            except queue.Empty:
                yield "error", {"message": f"Timeout ({int(timeout)} s) - the response was aborted.",
                                "code": "timeout"}
                return
            if raw is _EOF:
                break
            line = raw.decode("utf-8", errors="replace").strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            yield from map_cli_event(obj, state)
            if state.get("finished"):
                return

        # --- EOF without result ---
        proc.wait()
        err = b"".join(stderr_buf).decode("utf-8", errors="replace").strip()
        yield "error", {
            "message": _friendly_error(err) if err else
                       f"The claude CLI exited unexpectedly (exit code: {proc.returncode}).",
            "code": "cli_exit",
        }
    finally:
        kill_tree(proc)
