---
epic: epic_068
id: t883
title: Chat: BFF + önálló ablak + chronoquant MCP
assignee: ui_agent
status: done
blocks: [t889]
blocked_by: [t880]
---

## Goal
Dedikált chat oldal és popout ablak, ami úgy működik, mint a Claude chat: SSE stream, markdown/kód render, session (`--resume`), új beszélgetés. A `claude` CLI-t hívja (előfizetés, API-kulcs env kiszűrve), mint a `src/ui/chat/runner.py` / life_plan `chat_runner.py`. Hozzáférés: `chronoquant` MCP szerver (readonly SQL, modell/stratégia/riport lekérdezés) + szűk script-allowlist.

## Scope
`src/api/routers/chat.py`, `src/api/chat/**`, `src/cq_mcp/**` (új; NEM `src/mcp/`, árnyékolás), `frontend/src/pages/ChatPage.tsx`, `src/ui/chat/persona_prompt.md` (átköltözik), `.agent/skills/tv_sentiment_skill.md` hivatkozás. Epic_067 todo_t878 tartalma beépül, a ticket törlendő.

## Acceptance Criteria
- [x] Chat külön böngészőablakban is használható
- [x] Streamelt válasz, megszakítás, session folytatás
- [x] Claude eléri a readonly SQL-t és a riportokat; Edit/Write tiltott marad
- [x] Tesztek, ruff, pyright zöld

## Notes

### Elkészült (ui_agent)
**Backend** (`src/api/chat/`, `src/api/routers/chat.py`, `src/api/main.py`)
- `chat/runner.py`: a `src/ui/chat/runner.py` portja. `claude -p` stream-json, `--resume`, `STRIPPED_ENV` (API-kulcs kiszűrve), `--setting-sources ""`, `--tools Read,WebSearch,WebFetch`, `--allowedTools ...,mcp__chronoquant`, `--disallowedTools Edit,Write,NotebookEdit,Bash`, `--mcp-config` (http `/mcp/chronoquant`) + `--strict-mcp-config`. Új: `cancel` threading.Event (poll 0.25 s, process-fa kill), `stream_chat_async` (producer szál -> asyncio queue; a consumer lezárása/kliens-szakadás kilövi a claude-ot), `sse()`. MCP URL: env.json `chat.mcp_url` vagy env `CQ_MCP_URL` (dev/smoke másik porton).
- `routers/chat.py`: `POST /api/chat` (SSE: session/delta/thinking/tool/done/error), `GET /api/chat/config`. Nincs `claude` -> `503 binary_not_found` JSON; max 2 egyidejű turn -> `429 busy`. Megszakítás = kliens-szakadás (AbortController) -> process-fa kill. Szerver-oldali history nincs (CLI sajátja).
- `chat/persona_prompt.md`: átírva (MCP tool-lista, nincs Bash/Streamlit; a "nem találsz ki adatot / web = adat" szabályok változatlanok). A régi `src/ui/chat/persona_prompt.md` MÉG ott van (a Streamlit UI használja) -> **t889 törli**; az `utils._CHAT_DEFAULTS` (`persona_prompt_path`, `allowed_bash`) is legacy, az API runner a saját prompt-fájlját olvassa (utils.py-hoz nem nyúltam; t889/t891 takarítsa).
- `main.py`: chat router + `app.router.routes.append(mcp_route())` a SPA fallback előtt + `mcp_lifespan()` a lifespanben.

**MCP** (`src/cq_mcp/`, FastMCP streamable HTTP, in-process `/mcp/chronoquant`; stdio: `python -m cq_mcp.server`)
- 16 tool a blueprint §4.5 szerint: get_status, list_tables, query_sql (a `data_handling.store.readonly_query` modul védelmeivel), get_candles, get_predictions, get_latest_signal, get_active_strategy, get_trades(strat|journal|binance), get_equity, list_models, get_model, get_registry, list_reports, read_report, list_jobs, enqueue_sync (az egyetlen író; dedup, `worker_down` hiba ha nincs worker). A tool-ok blokkoló kódja `anyio.to_thread`-ben fut; kimenet <= 1000 sor / 50 KB. Újrahasznosítja az `api.services.*` réteget (egy RO kapcsolat/hívás).
- `list_reports/read_report`: `src/reports/store` (t884) még nem volt kész, ezért önálló, path-traversal-védett olvasó van a `tools_reports.py`-ban (`artifacts/<model>/reports/<slug>/`, `_doc_/reports/<slug>/`, manifest+report.md+charts). **t884 döntsön**: marad, vagy `reports.store`-ra cserélni.
- `pyproject.toml`: csak a `mcp` sor, **`mcp>=1.9,<2` pinnel** (az `mcp` 2.x átnevezte a FastMCP-t `MCPServer`-re és a `session_manager` API-t; a blueprint/life_plan minta 1.x). Telepítve: 1.30.0.

**Frontend** (`frontend/src/`; a t882 fájljaihoz nem nyúltam, a react-markdown/remark-gfm már a package.json-ban volt)
- `pages/ChatPage.tsx` (stub felülírva), `components/chat/{ChatPanel,ChatMarkdown,ChatDock}.tsx`, `lib/{chatStream,chatStore,useChat,popout}.ts`, `api/chat.ts`, `types/chat.ts`.
- Funkciók: markdown + GFM tábla + kódblokk (másolás gomb), streaming, Stop (abort), tool-chipek (olvas/ír), gondolkodás-blokk, session lista (localStorage `cq_chat_sessions` + üzenet-cache, `BroadcastChannel('cq-chat')` szinkron), új beszélgetés, folytatás (`--resume`), törlés (helyi).
- Popout: gomb -> `window.open('/chat?popout=1','cq-chat',...)`; `popout=1` esetén a ChatPage `fixed inset-0 z-50` overlayként takarja a Layout navját (Layout.tsx érintetlen).
- **epic_067 t878 beépült**: `ChatDock` = lebegő ikon + jobb oldali kicsúszó panel (nyitott állapot localStorage-ban; a panel zárva is mountolva marad, így a futó turn nem szakad meg). **Teendő (t882/orchestrátor): a `<ChatDock />`-ot be kell tenni a `Layout.tsx`-be** (1 sor, a `/chat` útvonalon magát elrejti). A t878 méretezés (80% zoom) és sync-alatti elérhetőség pontjai a Streamlit UI-ra vonatkoztak, amit a t889 töröl, így nem kerültek át; a React UI-ban nincs blokkoló rerun. A `todo_t878` ticket törölve.

**O-1 (TradingView-hangulat) — nem nyúltam hozzá**: `research/tv_report/`, `.agent/skills/tv_sentiment_skill.md`, `utils._CHAT_DEFAULTS["allowed_bash"]` érintetlen. A persona prompt jelzi, hogy a hangulat-elemzés shell nélkül jelenleg nem érhető el. Döntés után: (a) megtartás -> `sentiment_latest`/`sentiment_refresh` MCP tool (`cq_mcp/tools_sentiment.py`) + worker job `tv_report_fetch`, persona prompt visszabővítése, skill hivatkozás igazítása (Bash helyett MCP tool); (b) elengedés -> a skill törlése/átírása, `allowed_bash` kulcs törlése `utils.py`-ból, `research/tv_report/` törlése t886-ban.

### Futtatott parancsok
- `uv run ruff check src/api src/cq_mcp --fix` -> All checks passed
- `uv run pyright src/api src/cq_mcp` -> 0 errors
- `uv run pytest src/api src/cq_mcp -q` -> 54 passed (ebből új: 16 chat/runner/router fake `claude`-dal, 12 MCP tool + protokoll round-trip a valódi mountban)
- `npx tsc --noEmit` (frontend) -> tiszta
- Kézi smoke (uvicorn :8612, `CQ_MCP_URL` felülírással, valós `claude` 2.1.318 VS Code bináris): új session + `get_status` MCP tool-hívás (start/done események, thinking, delta, done), `--resume` a session-nel (a modell emlékezett a korábbi tool-ra). Megjegyzés: a `live.duckdb`-t külső író (sync) zárolta, ezért a tool `db: busy`-t adott — a hibakezelés élesben működött. A :8600-on egy másik (régebbi kódú) szerver fut, azt nem bántottam; a smoke-szervert leállítottam. A kliens-szakadás -> process kill a unit teszttel bizonyított (`test_async_consumer_close_kills_process`); élő process-számlálással nem sikerült megerősíteni (a CLI gyerekfolyamat nem látszott a Win32_Process listában).
- Nem futott: böngészős vizuális ellenőrzés (nincs vite dev/build futtatva; a t882 `frontend/dist`-jét nem írtam felül).

[validator] Done — 2026-11-06 (t892 epic-validáció)
Futtatva (egész-repo): ruff check src (All checks passed), pyright src (0 errors), pytest src -m 'not perf' (383 passed, 3 failed: élő-DB sanity — test_feat_ohlcv_quant_no_time_gap [14 időrés a live adatban], test_target_row_count_matches_features [target 3 120 401 vs feature 3 231 645, fw60 lemaradás + hiányzó sorok], test_predictions_score_range [nyers regressziós MFE score nem [0,1] — elavult feltételezés]; mind adatfüggő, kódhibára nem utal, az epic előtti ismert állapot), uv lock --check OK, frontend: tsc --noEmit + npm run build OK, registry_validator OK.
Valódi claude CLI-vel POST /api/chat (CQ_MCP_URL=:8698): session -> tool start/done mcp__chronoquant__get_status -> delta stream (3 231 663 OHLCV sor) — az MCP get_status eléri a BFF-et. Chat oldal screenshot: üres állapot rendben, konzolhiba nincs.
