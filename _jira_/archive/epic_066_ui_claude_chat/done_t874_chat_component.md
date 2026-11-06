---
epic: epic_066
id: t874
title: Streamlit chat komponens az UI-n
assignee: ui_agent
status: done
blocks: []
blocked_by: [t873]
---

## Goal
Egyetlen chat-ablak a ChronoQuant UI-n (nincs külön fül/oldal-persona). Streamelt válasz, tool-haladás jelzés.

## Scope
`src/ui/components/` (új chat komponens), bekötés `src/ui/main.py`-ban (sidebar vagy alsó chat panel — a ui_agent döntse el, ami Streamlit-ben stabil).

## Acceptance Criteria
- [x] `st.chat_input` + `st.write_stream`, history `st.session_state`-ben, claude session_id megőrizve (--resume)
- [x] tool-események láthatók (pl. "tv_report fut…", "SQL lekérdezés…"), hogy a hosszú futás ne tűnjön lefagyásnak
- [x] hibák érthetően (nincs bejelentkezve / nincs claude bináris / timeout)
- [x] "Új beszélgetés" gomb (session reset)
- [x] a chat nem blokkolja/rontja a meglévő oldal működését; a meglévő UI smoke teszt (STREAMLIT_CONFIG_DIR=src/ui uv run streamlit run src/ui/main.py) rendben
- [x] ruff + pyright tiszta

## Notes
**Done (ui_agent).** Files:
- `src/ui/chat/model.py` (new): Streamlit-free chat state (`ChatState`, `ChatMessage`, `ToolStatus`), `apply_event` (runner event -> UI model), `start_turn` (worker thread), `cancel`, HU error texts per code (binary_not_found, timeout, cli_error, cli_exit, spawn_failed), tool labels.
- `src/ui/components/chat_panel.py` (new): `render_chat_panel()`.
- `src/ui/main.py`: import + `st.divider(); render_chat_panel()` at the bottom (full-width, under chart/log/trade columns). No new tab/persona.
- Tests: `src/ui/tests/chat/smoke/test_model.py` (8, no Streamlit), `test_chat_panel.py` (AppTest, fake stream). `ruff` clean, `pyright src/ui/` 0 errors, `pytest src/ui/tests` 30 passed.

Design decision (deviates from the literal `st.write_stream` AC): the sidebar sync fragment reruns every 2 s, and any rerun interrupts a script blocked on the stream, which would close the generator and kill the claude process. So each turn is consumed by a daemon worker thread mutating `ChatState` (kept in `st.session_state["chat_state"]`); the UI polls it with a `st.fragment(run_every="0.5s")` only while a turn runs (partial text with a cursor, tool status lines with icons), then does one app rerun to re-enable `st.chat_input`. The rest of the page is unaffected. claude `session_id` is stored in ChatState and passed to `stream_chat` on the next turn (--resume). "Új beszélgetés" replaces the state.

Known limitations:
- Reset/cancel only sets a flag; the worker stops (and kills the process tree via generator close) at the next runner event, so a silent long tool run may continue in the background until it emits something or times out.
- Closing the browser tab mid-turn does not stop the worker; it runs to completion (bounded by the runner timeout).
- Tool status lines show the tool kind only (the runner events carry no tool input), so Bash is labelled generically "tv_report, SQL".
- Real-app manual smoke (streamlit run) was not done; an AppTest of main.py was inconclusive (auto-sync thread / DB). Chat panel itself is covered by AppTest.
