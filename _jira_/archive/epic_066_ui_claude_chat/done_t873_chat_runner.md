---
epic: epic_066
id: t873
title: Claude CLI chat_runner modul
assignee: ui_agent
status: done
blocks: [t874]
blocked_by: []
---

## Goal
Headless `claude -p` futtató modul a Streamlit UI-hoz, a life_plan `backend/chat_runner.py` mintájára
(olvasd el: d:\repos\life_plan\backend\chat_runner.py), de szinkron generátorként (Streamlit st.write_stream-hez), FastAPI nélkül.

## Scope
`src/ui/chat/runner.py` (új). Config: `src/utils.py`-on keresztül (nincs nyers JSON olvasás).

## Acceptance Criteria
- [x] claude bináris feloldás: CLAUDE_BIN env → PATH → VS Code extension glob; érthető hiba, ha nincs
- [x] `--output-format stream-json --verbose --include-partial-messages`, `--resume <session_id>`
- [x] ANTHROPIC_API_KEY / ANTHROPIC_AUTH_TOKEN kiszűrve a gyerek env-ből
- [x] `--tools Read,WebSearch,WebFetch,Bash` és `--allowedTools` szűk Bash allowlisttel (`Bash(uv run python research/tv_report/*)`, `Bash(uv run python <sql helper>*)`); Edit/Write nincs engedélyezve; `--permission-mode dontAsk`
- [x] cwd = repo gyökér; `--append-system-prompt` a t876 persona promptból (olvassa be fájlból)
- [x] stream-json → események: session, delta, tool(start/done/error), done, error; timeout konfigurálható (default magas, tv_report miatt), kill a teljes process tree-n (Windows taskkill /T), CREATE_NO_WINDOW
- [x] tesztek: esemény-mapping, parancsépítés (nincs Edit/Write, env szűrés), nem hívja a valódi CLI-t
- [x] ruff + pyright tiszta

## Notes

**Done (ui_agent).** Files:
- `src/ui/chat/__init__.py`, `src/ui/chat/runner.py` (new)
- `src/utils.py`: added `load_chat_config()` (defaults overlaid with optional `chat` section of config/env.json: model=sonnet, timeout_s=1800, persona_prompt_path=src/ui/chat/persona_prompt.md, allowed_bash list) and `repo_root()`.
- `src/ui/tests/chat/smoke/test_runner.py` (17 tests, fake process, real CLI never called).

Public API (`ui.chat.runner`): `stream_chat(message, session_id=None) -> Iterator[(event, data)]`
(sync generator; events: session{id}, delta{text}, tool{name,status start|done|error,id}, done{session_id,duration_ms,cost_usd}, error{message,code}).
Runtime failures (missing binary code=binary_not_found, timeout, cli_error, cli_exit, spawn_failed) are yielded as `error` events, not raised.
Also: `resolve_claude_bin`, `build_command`, `build_env`, `map_cli_event`, `load_persona_prompt`, `kill_tree`, `ChatError`, `ChatBinaryNotFound`.

Decisions:
- Message goes via stdin; persona prompt read from file on every call (fallback short prompt + warning if the file is missing).
- `--tools Read,WebSearch,WebFetch,Bash`; `--allowedTools` = Read,WebSearch,WebFetch + the two Bash(...) patterns (no bare Bash); extra `--disallowedTools Edit,Write,NotebookEdit` as defense in depth; `--permission-mode dontAsk`.
- No `--setting-sources` / MCP flags (unlike life_plan sample): cwd is repo root so CLAUDE.md loads; `--tools` limits the tool set regardless of project settings. Project .claude settings could still widen Bash allow rules - t877 may check.
- Blocking readline replaced by a reader thread + queue so the timeout works in a sync generator; closing the generator (st rerun) kills the process tree (taskkill /F /T on Windows, CREATE_NO_WINDOW).
- Streamlit: remember `session` / `done.session_id` in st.session_state and pass as session_id next turn.
- Quality: `uv run ruff check src/ui/ src/utils.py --fix` clean, `uv run pyright src/ui/` 0 errors, `uv run pytest src/ui/tests` 17 passed.

[validator] 2026-11-06 - validated (done). Fix applied: .claude/settings.json (`Bash(*)`, `Edit(*)`, `Write(*)`) and settings.local.json (bare `Bash`) plus the user-level `Bash(uv run *)` widened the chat permissions (a non-allowlisted `echo`/`hostname` ran in the real CLI; project Stop/UserPromptSubmit hooks would also fire). runner.build_command now passes `--setting-sources ""` and `--strict-mcp-config`; re-tested with the real CLI: off-allowlist Bash refused, allowlisted SQL helper works. Regression test `test_project_settings_not_loaded` added.
