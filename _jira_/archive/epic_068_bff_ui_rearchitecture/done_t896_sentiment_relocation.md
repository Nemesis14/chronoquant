---
epic: epic_068
id: t896
title: TV-hangulat áthelyezése src/sentiment/-be + MCP tool + worker job
assignee: ui_agent
status: done
blocks: [t890]
blocked_by: [t886, t889]
---

## Goal
A `research/tv_report` két működő scriptje és a `tradingview` collector adja a chat TradingView-hangulat funkcióját. A research/ törlése után ennek új, rendezett helye kell.

## Scope
`research/tv_report/*` (a 2 működő script) -> `src/sentiment/tv_report/`; a tradingview collector marad a `data_handling/collectors/tradingview` alatt (vagy `src/sentiment/` alá költözik, ha a hívó-leltár ezt indokolja, döntés a Notes-ba). MCP tool(ok) a `src/cq_mcp/` alatt (`sentiment_latest`, `sentiment_refresh`), `refresh` worker job formában (a `src/worker/handlers` mintájára). `.agent/skills/tv_sentiment_skill.md`, a persona prompt (`src/api/chat/persona_prompt.md`) és `utils.allowed_bash`/chat config frissítése az új útvonalakra, a Bash tiltás megmarad (a Claude MCP-n keresztül éri el). A kimeneti riportok (hangulat-jelentés) mehetnek az új report-formátumba (`_doc_/reports/` vagy artifacts), a blueprint szerint.

## Acceptance Criteria
- [x] A chatben a TV-hangulat kérés MCP toolon át működik
- [x] `research/` nem hivatkozódik sehonnan (kódból/configból; a maradék említés csak történeti doksi, lásd Notes)
- [x] Tesztek, ruff, pyright zöld

## Notes
[orchestrator] A két tv_report script (`fetch_report_data.py`, `author_backtest.py`) soha nem volt gitben (gitignore-os), a lemezen lévő példány az egyetlen. Biztonsági másolat: `_backup_tv_report_scripts/` (repo gyökér). Másolással költöztesd `src/sentiment/tv_report/`-be (nem `git mv`), commitra kész állapotban; a `research/tv_report/` és a `_backup_tv_report_scripts/` mappát csak a működés ellenőrzése után töröld. A `.gitignore` tv_report sorát az új kimeneti útvonalra (pl. `data/tv_report/`) cseréld.

### [ui_agent] Elvégzve (2026-11-06)
**Csomag** `src/sentiment/tv_report/` (másolással, a viselkedés változatlan): `paths.py` (kimenet `data/tv_report/`, gitignore-os `data/` alatt), `fetch.py` (régi `fetch_report_data.py`; `run_fetch()` + CLI `python -m sentiment.tv_report.fetch`, `make_client` seam a teszteléshez), `author_backtest.py` (lusta kliens, függvényekre bontva, CLI `python -m sentiment.tv_report.author_backtest discover|run`), `summary.py` (`latest_digest`). print -> logging, sys.path-hack ki, típusannotációk, útvonal csak a `utils.repo_root()`-on át. A `collectors/tradingview` marad a `data_handling` alatt (a hívó-leltár nem indokolta a költöztetést; most a `sentiment` és a (t894 után is megmaradó) tradingview collector kapcsolata egyirányú).
**Worker**: új kind `sentiment_refresh` (`worker/queue.py` JOB_KINDS, `worker/handlers/sentiment_refresh.py`, regisztráció a `handlers/__init__.py`-ban; `POST /api/jobs` Literal bővítve). Params: `hours` 1..720 (24), `pages` 1..5 (3), a result a riport fejléce (`ideas` nélkül).
**MCP** (`cq_mcp/tools_sentiment.py`, 18 tool): `sentiment_latest(max_ideas, desc_chars)` (olvasó) és `sentiment_refresh(hours, pages)` (worker jobot enqueue-ol, dedup, `worker_down`). **Jogosultság: az író tool lista bővült: `enqueue_sync` + `sentiment_refresh`** (mindkettő csak worker jobot ütemez, DuckDB-t nem ír). Blueprint (`_doc_/_plans_/068_bff_ui.md` §4.5 tool-lista, §5.3 job kind) és a szerver INSTRUCTIONS frissítve.
**Chat**: `persona_prompt.md` (hangulat-workflow MCP-n át, 2 író tool), `.agent/skills/tv_sentiment_skill.md` átírva (Bash nélkül; author_backtest = csak CLI). `utils.py`: `_CHAT_DEFAULTS` = `model`, `timeout_s` (az `api/chat/runner` ténylegesen ezeket + `mcp_url`-t használja); `persona_prompt_path` és `allowed_bash` törölve (a runner a modul melletti prompt-fájlt olvassa). `config/env.json`-ban nem volt `chat` szekció, nem módosítottam. `.gitignore`: a három research sor törölve, explicit `data/tv_report/` hozzáadva. `_doc_/research/tv_report.md` frissítve.
**Teszt**: `sentiment/tv_report/tests/test_fetch.py` (fake kliens: számlálók, dedup, handler regisztráció/clamp, TP/SL regex, evaluate), `cq_mcp/tests` (`test_sentiment_tools`, tool-lista). ruff tiszta, pyright 0 hiba, pytest `src/sentiment src/api src/cq_mcp src/worker`: 92 passed.
**Valós futás**: `python -m sentiment.tv_report.fetch --hours 24 --pages 1` OK (100 nyers -> 33 BTC trade idea, 15 long / 5 short címkézett); `sentiment_latest` a valós `data/tv_report/latest.json`-on működik. A worker-processzen át nem futtattam (a worker nem volt indítva), a handler a fake klienssel tesztelt.
**Törölve** a működés ellenőrzése után: `research/` (teljes) és `_backup_tv_report_scripts/`.
**Nyitott (t891, doksi)**: történeti említések maradtak: `_doc_/0000_project_overview.md`, `PLAN_tv_market_sentiment.md`, blueprint §6.3/D-8 (O-1 már eldőlt), `CLAUDE.md` delegation sor. A `.agent/agents/ui_agent.md` scope-ja ne feledje a `src/sentiment/`-t (a delegation táblában még nincs).

[validator] Done — 2026-11-06 (t892 epic-validáció)
Futtatva (egész-repo): ruff check src (All checks passed), pyright src (0 errors), pytest src -m 'not perf' (383 passed, 3 failed: élő-DB sanity — test_feat_ohlcv_quant_no_time_gap [14 időrés a live adatban], test_target_row_count_matches_features [target 3 120 401 vs feature 3 231 645, fw60 lemaradás + hiányzó sorok], test_predictions_score_range [nyers regressziós MFE score nem [0,1] — elavult feltételezés]; mind adatfüggő, kódhibára nem utal, az epic előtti ismert állapot), uv lock --check OK, frontend: tsc --noEmit + npm run build OK, registry_validator OK.
