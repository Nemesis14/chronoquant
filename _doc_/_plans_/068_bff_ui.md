# Epic 068 — BFF/UI újraarchitektúra: Blueprint (t879)

Státusz: **megvalósítva (epic_068, t880–t891); kontraktus** — az implementáció ezt a tervet követi, az eltérések a lenti "Megvalósítás: eltérések" szakaszban vannak. Kód-referencia a kész állapotról: `_doc_/database_and_code_doc/8300_api.md` … `8360_scripts.md`. Ha a kontraktus változik, előbb ezt a fájlt kell javítani (code_doc_agent), nem csendben eltérni. A szöveg többi része (különösen §1, §6 törlési lista, §8) az eredeti terv; a törlések végrehajtva (commit `73f2156` után), a `src/ui/`, `research/`, `analyst/`, Quarto megszűnt.
Készítette: code_doc_agent, t879. Dátum: 2026-11-06.

Minta: `D:\repos\life_plan` (`backend/main.py`, `routers/chat.py`, `chat_runner.py`, `frontend/src`, `scripts/*.ps1`). Jelenlegi UI (lefedendő nézetek): `src/ui/main.py`, `data.py`, `binance_data.py`, `sync.py`, `sync_runner.py`, `trading_runner.py`, `components/*`, `chat/*`.

## 0. Összefoglaló döntések

| # | Döntés | Indok (röviden) |
|---|--------|-----------------|
| D-1 | **Chart-spec: Plotly JSON** (megerősítve) | §3.1: az `analyst/lib/plot_utils.py` **matplotlib/seaborn** (statikus kép), tehát egyik spec-et sem lehet átvenni; a projektben viszont már van `plotly` függőség és Plotly-s kód (`src/ui/components/charts.py`: gyertya, dual-axis, shape/overlay). Vega-Lite gyertyára/overlayre gyengébb, és új függőség lenne |
| D-2 | **Az MCP csomag neve `src/cq_mcp/`, NEM `src/mcp/`** (eltérés az epic szövegétől) | A `pyproject` `pythonpath = ["src"]` → egy `src/mcp/` árnyékolná a PyPI `mcp` SDK-t (`from mcp.server.fastmcp import FastMCP` elromlik) |
| D-3 | Natív `/api/*` prefix minden routeren (nincs `StripApiPrefixMiddleware`) | Egyszerűbb, mint a life_plan; a Vite proxy nem írja át az útvonalat |
| D-4 | Az API **nem ír** DuckDB-t és **nem importál** `modeling`/`strategy`/`trading`/`ui` modult | §7; az írást csak a worker végzi |
| D-5 | Worker = külön processz, job-queue **SQLite** fájlban (`database/_jobs/jobs.sqlite`, WAL) | Többprocesszes biztonságos, nincs plusz DuckDB-zár, nincs új függőség (stdlib `sqlite3`) |
| D-6 | Chat = a meglévő, tesztelt `src/ui/chat/runner.py` logika **portolása** (`claude -p` headless, stream-json), async SSE-be csomagolva | Nem újraírás: `map_cli_event`, `build_command`, `kill_tree` tesztekkel együtt költözik |
| D-7 | Élő kereskedés vezérlés **nincs** (scope-on kívül); csak a trade journal és a stratégia-kártyák olvasása | User döntés |
| D-8 | **Nyitott, user-döntést igényel (O-1):** `research/tv_report/` a chat piaci-hangulat funkciójának a motorja | §6.3: a research/ teljes törlése ezt elvágná |

### Megvalósítás: eltérések a tervtől (t891 állapot)

| Téma | Terv | Valóság |
|------|------|---------|
| Globális SSE `/api/stream` (§2.1) | worker/job/data események egy csatornán | **Nincs.** Job-szintű SSE: `GET /api/jobs/{id}/events`; a frontend `usePolling`-gal frissít (`/api/status` 4 s, chart 60 s) |
| Jobs/reports router | mindig jelen | `OPTIONAL_ROUTERS = ("jobs", "reports")` — csak ha a modul létezik (`api.main`) |
| Extra végpontok | – | `GET /api/market/config`, `/market/tables`, `/api/trades/binance`, `/api/jobs/worker`, `/api/jobs/auto-sync` (GET/PUT), `/api/chat/config`; Binance fillek cache-eltek (t893) |
| MCP `query_sql` aliasok (§4.5) | `live.`, `snap.`, `model.`, `strat.`, `reg.` | `live.`, `lab.` (benne `lab.strat.*`, `lab.snap.*`, `lab.model.*`), `reg.` |
| MCP mount | `/mcp/chronoquant` | Igaz, de route az app routerén (nem `Mount`, így nincs 307 slash-redirect), a SPA fallback előtt; stdio: `python -m cq_mcp.server` |
| Chat jogosultság (§4.4) | Read/WebSearch/WebFetch + MCP | Igaz; `--disallowedTools Edit,Write,NotebookEdit,Bash`, `--permission-mode dontAsk`, max 2 párhuzamos chat (429 `busy`) |
| Chat konfig | `load_chat_config` több kulccsal | `utils._CHAT_DEFAULTS` = `model`, `timeout_s`; a persona a modul melletti `persona_prompt.md` (a `persona_prompt_path`, `allowed_bash` törölve) |
| Scriptek (§5.5) | `start_chronoquant.ps1`, `stop_chronoquant.ps1`, `start_worker/stop_worker.ps1`, `build_frontend.ps1`, `dev.ps1`, `install_autostart.ps1` | `dev.ps1`, `start.ps1` (`-NoBrowser`, `-Build`), `stop.ps1`, `install_autostart.ps1`, `_common.ps1`; a worker indítása/leállítása a start/stop része, a frontend build a `start.ps1 -Build` |
| Job kindek (§5.3) | `sync_full`, `sync_predictions` | + `sentiment_refresh` (t896) |
| O-1 `research/tv_report` | nyitott | Eldőlt (A): költözés `src/sentiment/tv_report/` + MCP `sentiment_latest`/`sentiment_refresh` (2. író tool) |
| O-2 `src/trading/` | nyitott | Csak `src/trading/live/journal.py` maradt; az order-logika törölve (t897) |
| Törölt DB-oldali táblák | – | altdata / universe / analyst_signal kutatási táblák törölve (t898) |

---

## 1. Célmappa-struktúra

```
src/
  utils.py                      # config gateway — MARAD, minden config ezen át
  data_handling/                # MARAD (t887 audit); az API ebből csak olvasó helpereket importál (§7)
  modeling/ strategy/ trading/  # az API NEM importálja (t888/t889 saját sorsa)
  api/                          # FastAPI BFF  (t880, t883)
    main.py                     #   app, lifespan, CORS (csak dev: http://localhost:5173), SPA fallback, /mcp mount
    deps.py                     #   read-only DuckDB connection factory + retry (§5.4), config
    errors.py                   #   egységes hibaformátum (§2.0)
    routers/
      health.py market.py predictions.py trades.py equity.py strat.py
      registry.py jobs.py chat.py reports.py logs.py stream.py
    services/                   #   az ui/data.py + binance_data.py logikájának portja, pandas nélküli JSON-kimenettel
      market.py predictions.py trades.py strat.py registry.py binance.py logs.py
    chat/
      runner.py                 #   port: src/ui/chat/runner.py (Popen+pump szál → async SSE)
      persona_prompt.md         #   port: src/ui/chat/persona_prompt.md (tartalom frissítendő, §4)
    tests/                      #   smoke: TestClient + tmp DuckDB; chat: a meglévő test_runner/test_model portja
  worker/                       # háttér-processz  (t881)
    __main__.py                 #   `python -m worker` (belépési pont, ciklus + heartbeat)
    queue.py                    #   SQLite job-tár (jobs, settings, heartbeat)
    handlers/                   #   sync_full.py, sync_predictions.py  (a src/ui/sync.py logikájának portja)
    scheduler.py                #   auto-sync (zárt perc + delay), a ui/sync_runner.py portja
    logs.py                     #   job-log fájl író (logs/jobs/<job_id>.log)
    tests/
  cq_mcp/                       # chronoquant MCP szerver  (t883)  — FastMCP, streamable HTTP, in-process az API-ban
    server.py                   #   MCP_SERVER = FastMCP("chronoquant"); mount: /mcp/chronoquant
    tools_data.py tools_models.py tools_reports.py tools_jobs.py
  reports/                      # report-formátum + builder  (t884) — az API csak olvasni használja (reports.store)
    schema.py                   #   pydantic: Manifest, ChartRef, TableRef
    builder.py                  #   ReportBuilder (§3.4)
    store.py                    #   list/read (path-traversal védelemmel), owner→gyökér feloldás
    theme.py                    #   a plot_utils palettája (LONG/SHORT/ACCENT/NEUTRAL) Plotly template-ként
    builders/                   #   modell-riportok: sampling.py, feature_engineering.py, hyperparameter_search.py, strategy.py
frontend/                       # React 18 + Vite 5 + TypeScript + Tailwind 3  (t882, t883, t884)
  package.json vite.config.ts tsconfig.json tailwind.config.js index.html
  src/
    main.tsx App.tsx index.css
    api/        client.ts market.ts predictions.ts trades.ts equity.ts strat.ts registry.ts jobs.ts chat.ts reports.ts
    lib/        sse.ts (SSE parser, a life_plan lib/chatStream.ts mintájára), format.ts, usePolling.ts
    types/      api.ts chat.ts reports.ts
    pages/      DashboardPage.tsx  StrategyPage.tsx  TradesPage.tsx  ModelsPage.tsx  ReportsPage.tsx  JobsPage.tsx  ChatPage.tsx
    components/ layout/ charts/(PredictionPriceChart, EquityChart, PlotlyFigure[lazy]) trade/ strat/ jobs/ chat/ reports/
scripts/                        # PowerShell indítók (a life_plan mintájára, §5.5)
  _common.ps1  start_chronoquant.ps1  stop_chronoquant.ps1  start_worker.ps1  stop_worker.ps1
  dev.ps1  install_autostart.ps1  build_frontend.ps1
_doc_/
  research/                     # t886: research/ rövid md összefoglalók (a már ott lévő 13 md mellé)
  reports/<slug>/               # ÁLTALÁNOS (nem modellhez kötött) elemzések — az új gyökér, a models_doc/ helyére (§3.2)
artifacts/<model_id>/reports/<slug>/   # MODELL-specifikus riportok (§3.2)
database/_jobs/jobs.sqlite      # job-queue (gitignore-olt: /database/)
logs/                           # api.*.log, worker.*.log, jobs/<job_id>.log
```

Portok: API + buildelt SPA: `127.0.0.1:8600`; Vite dev: `5173` (proxy `/api` és `/mcp` → `8600`, **rewrite nélkül**).
`frontend/dist` buildelt SPA-t az API szolgálja ki (a life_plan SPA-fallback mintájára; ismeretlen `/api/*` → 404 JSON, nem `index.html`).

Új függőségek (t880/t883, `uv add`): `fastapi`, `uvicorn[standard]`, `mcp` (FastMCP). Frontend: `react`, `react-dom`, `react-router-dom`, `react-markdown`, `remark-gfm`, `plotly.js-dist-min` + `react-plotly.js` (lazy chunk), `lucide-react`; dev: vite, typescript, tailwindcss, `@vitejs/plugin-react`. Nincs `axios` (sima `fetch`), nincs `recharts`.

---

## 2. REST / SSE kontraktus

### 2.0 Általános szabályok

- Prefix: `/api`. Minden válasz JSON, kivéve az SSE végpontokat (`text/event-stream`, `Cache-Control: no-cache`, `X-Accel-Buffering: no`).
- **Idő:** kimenet mindig UTC ISO-8601 `"2026-06-14T10:21:00Z"`; bemenet ugyanez. (A DB naív `YYYY-MM-DD HH:MM:SS` UTC — a service réteg konvertál.) `NaN/Inf → null`.
- **Hiba:** `{ "detail": "<ember-olvasható>", "code": "<gép-kód>" }` + HTTP státusz. Kódok: `not_found` 404, `bad_request` 400/422, `db_busy` 503, `no_data` 404 (üres tábla/hiányzó artifact — a lista-végpontok ilyenkor `200` + üres tömb), `binance_unavailable` 503, `worker_down` 503, `conflict` 409.
- **Lapozás** ahol nagy lehet: `limit` (default 200, max 5000) + `offset`; a válasz `{ "items": [...], "total": N }`.
- **Csak olvasás** minden GET; írás kizárólag a `jobs` csoportban (enqueue/cancel/auto-sync).
- Asset: single-asset (`solusdt`); az API a `utils.load_asset_config(None)`-ból veszi. Query-paraméter `asset_id` **nincs** v1-ben.

### 2.1 health / status / stream

| Metódus + path | Válasz vázlat |
|---|---|
| `GET /api/health` | `{ "status": "ok", "time": "…Z" }` (indító script ezt várja) |
| `GET /api/status` | `{ "asset_id", "symbol", "interval", "live": { "ohlcv": {"rows", "latest_open_time"}, "features": {…}, "predictions": {…} }, "models": { "long": {"model_id"}, "short": {"model_id"} }, "worker": { "alive": bool, "last_heartbeat": "…Z" }, "sync": { "running": bool, "job_id": str\|null, "last_finished_at": "…Z"\|null, "last_status": "succeeded\|failed\|null", "auto_sync": bool }, "db": { "state": "ok\|busy" } }` — a régi `table_health` + oldalsáv szinkron-státusz |
| `GET /api/stream` (SSE) | Globális eseménycsatorna, 15 mp heartbeat. Események: `worker {alive}`, `job {id, kind, status}`, `data {table, latest_open_time}` (új gyertya/predikció), `ping {}`. A frontend ezekre invalidál/újratölt a perces polling helyett |

### 2.2 market (gyertyák, fő grafikon)

| Metódus + path | Válasz vázlat |
|---|---|
| `GET /api/market/config` | `{ "asset_id", "symbol", "interval", "windows": ["24h","8h","4h"], "default_window": "24h" }` |
| `GET /api/market/candles?hours=24&tf=1m\|5m\|15m` | `{ "tf", "from", "to", "candles": [{"t","o","h","l","c","v"}] }` (a `charts._resample_ohlcv` logika) |
| `GET /api/market/chart?window=24h` | **A fő dashboard egyetlen hívása** (percenként): `{ "window", "focus_hours", "from", "to", "bars": [{"t","o","h","l","c","long_pct","short_pct","long_raw","short_raw"}], "thresholds": {"long_entry": 0.98\|null, "short_entry": 0.94\|null}, "active_trade": {"side","entry_price","stop_loss","take_profit"}\|null }`. A `*_pct` a rank-percentil (`utils.apply_rank_percentile`), `*_raw` a nyers pred. A jelölők (long/short szignál) kliensoldalon számolódnak a küszöbökből |
| `GET /api/market/tables` | `[{"table","table_name","exists","rows","latest_time"}]` (régi `table_health`) |

A grafikon **vizuális specifikációja** a `src/ui/components/charts.py` (3 sor: long pct 20% / gyertya 60% / short pct 20%, küszöbvonalak, long/short jelölők, aktív trade overlay, `focus_hours` ablak). A t882 ezt portolja TS-be (`PredictionPriceChart.tsx`), **mielőtt** a t889 törli.

### 2.3 predictions

| Metódus + path | Válasz vázlat |
|---|---|
| `GET /api/predictions/history?hours=24` | `{ "items": [{"t","close","long_pct","short_pct","long_raw","short_raw"}] }` |
| `GET /api/predictions/latest` | `{ "t","close","long_pct","short_pct","long_model_id","short_model_id" }` \| `404 no_data` |
| `GET /api/predictions/signals?hours=24` | `{ "items": [{"t","side":"long\|short","pct","price"}] }` (küszöb-átlépések az aktív stratégia cutoffjaival) |

### 2.4 trades

| Metódus + path | Válasz vázlat |
|---|---|
| `GET /api/trades/closed?limit=500&source=strat` | Zárt trade-ek a long+short aktív `strat."<session>__trades"` táblákból: `{ "items": [{"entry_time","exit_time","side","entry_price","exit_price","fact_log_return","tp_lr","sl_lr","exit_reason","tp_spec","sl_spec","session_id"}], "total" }` |
| `GET /api/trades/active` | Nyitott pozíció: a journal `trading_positions` (status OPEN/LONG/SHORT) legfrissebbje; üres journal esetén Binance fallback. `{ "side","entry_time","entry_price","quantity","stop_loss","take_profit","source":"journal\|binance" }` \| `404 no_data` |
| `GET /api/trades/journal/positions?status=open\|closed&limit=` | `trading_positions` sorok (`position_id, run_id, side, status, entry_time, exit_time, entry_price, exit_price, quantity, pnl_usdt, exit_reason`) |
| `GET /api/trades/journal/orders?limit=200` | `trading_orders` sorok (a `request_json/response_json` mező nélkül; az külön: `?full=true`) |
| `GET /api/trades/journal/signals?limit=50` | `trading_signals` (`bar_open_time, decision, reason, state…`) |
| `GET /api/trades/journal/errors?limit=100` | `trading_errors` (`error_time, component, error_type, message`) |
| `GET /api/trades/journal/runs?limit=20` | `trading_runs` |
| `GET /api/trades/binance?limit=200` | Utolsó Binance fillek (futures → spot fallback), percre csoportosítva (`trade_panel._group_trades_by_minute` logika): `{ "items": [{"t","side","price","qty","realized_pnl"}], "source":"futures\|spot" }`; kulcs/hálózati hiba → `503 binance_unavailable` (a UI szürkén jelzi, nem hibázik) |

A journal-táblák a `config/trading.json → db_path` DuckDB-ben vannak; az API **raw read-only SQL-lel** olvassa, `trading.live.journal`-t nem importálja (§7). Hiányzó tábla → üres lista.

### 2.5 equity

| Metódus + path | Válasz vázlat |
|---|---|
| `GET /api/equity?session=active&direction=both\|long\|short` | `{ "session_ids": {"long","short"}, "points": [{"t","cum_fact_log_return","direction"?}], "summary": {…} }` az `strat."<session>__equity"` táblákból (`direction=both` esetén két sor-sorozat `series: {long:[…], short:[…]}`) |
| `GET /api/equity/summary` | `{ "long": {"n_trades","win_rate","total_fact_log_return","compounded_return_pct","avg_hold_minutes"}, "short": {…} }` (régi `backtest_summary`) |

### 2.6 strat

| Metódus + path | Válasz vázlat |
|---|---|
| `GET /api/strat/active` | A "Aktív Stratégia" kártyák adata: `{ "long": {"session_id","fit_period":{"start","end"},"entry_cutoff","tp_spec","sl_spec","max_hold_minutes","metrics":{"n_trades","win_rate","total_fact_log_return","compounded_return_pct","avg_hold_minutes"}}, "short": {…} }` (`load_long_short_strategies` + `load_strategy_artifact`) |
| `GET /api/strat/sessions` | Elérhető stratégia-session-ök (`reg.strategies` + `lab.strat` táblák): `[{"session_id","model_id_long","model_id_short","status","created_at"}]` |
| `GET /api/strat/{session_id}/artifact` | A `strategy_artifact.json` tartalma |
| `GET /api/strat/{session_id}/trades?limit&offset` · `/equity` · `/cutoffs` · `/summary` | A megfelelő `strat."<session>__<kind>"` tábla JSON-ban |
| `GET /api/strat/{session_id}/grid?top=50&sort=total_fact_log_return` | `grid_results` legjobb N setup |

`session_id` itt a `strat."<id>__…"` tábla-prefix; az artifact útvonal feloldása a `reg.artifacts`-ból vagy `artifacts/<model_id>/strategy/` konvencióból (jelenlegi `data._session_artifact_path` logika).

### 2.7 registry (csak olvasás)

| Metódus + path | Válasz vázlat |
|---|---|
| `GET /api/registry/active` | `{ "live_models": {"long","short"}, "strategy_sessions": {"long","short"}, "pending_deployments": [...] }` |
| `GET /api/registry/{table}?limit=200&filter=` | `table ∈ {assets, snapshots, feature_sets, models, search_runs, strategies, deployments, artifacts}` (whitelist, **nem** szabad SQL); `{ "items": [...], "total" }` |
| `GET /api/registry/models/{model_id}` | `{ "registry": {…reg.models sor}, "manifest": {…artifacts/<id>/manifest.json}, "params": {…}, "search_best": {…}, "feature_count": N, "artifacts": [{"kind","path","exists"}], "reports": [manifest-lista, §3.6] }` |
| `GET /api/registry/models/{model_id}/features` | `features.json` tartalma |

### 2.8 jobs (worker) — lásd §5

| Metódus + path | Válasz vázlat |
|---|---|
| `POST /api/jobs` body `{ "kind": "sync_full\|sync_predictions", "params": {} }` | `202 { "job": {…}, "deduplicated": bool }` — ha már van `queued`/`running` ugyanilyen `kind`, **azt** adja vissza (`deduplicated: true`), nem hoz létre újat. Worker nem él → `503 worker_down` |
| `GET /api/jobs?status=&kind=&limit=50` | `{ "items": [Job] }` legújabb elöl |
| `GET /api/jobs/{id}` | `Job = { "id", "kind", "status": "queued\|running\|succeeded\|failed\|canceled", "created_at", "started_at", "finished_at", "params", "result": {"ohlcv_rows_before","ohlcv_rows_after","inserted_ohlcv_rows","start_time","end_time"}\|null, "error": str\|null, "log_size": int }` |
| `GET /api/jobs/{id}/log?from=0` | `{ "offset": <új byte-offset>, "lines": ["…"], "done": bool }` (pollolható) |
| `GET /api/jobs/{id}/events` (SSE) | `status {status}`, `log {offset, line}` (újracsatlakozás: `?from=<offset>`), `done {status, result, error}`. Lezárul a `done` után |
| `DELETE /api/jobs/{id}` | Csak `queued` → `canceled`. `running` → `409 conflict` (a DuckDB-sync nem szakítható meg biztonságosan) |
| `GET /api/jobs/worker` | `{ "alive": bool, "pid", "started_at", "last_heartbeat", "current_job_id" }` |
| `GET /api/jobs/auto-sync` · `PUT /api/jobs/auto-sync` body `{ "enabled": bool }` | `{ "enabled", "delay_seconds", "next_due_in_s": int\|null }` |

### 2.9 logs

`GET /api/logs/{source}?lines=200` — `source ∈ {api, worker}`; `{ "lines": [{"ts","level","logger","msg"}] }` (a régi `log_panel` helyett; a job-log a `jobs/{id}/log`).

### 2.10 chat — lásd §4

| Metódus + path | |
|---|---|
| `POST /api/chat` body `{ "message": str(1..8000), "session_id": str\|null }` | SSE: `session`, `delta`, `thinking`, `tool`, `done`, `error` (§4.2). Előfeltétel-hiba (nincs `claude` bináris) → `503 {code:"binary_not_found"}` JSON, nem SSE |
| `GET /api/chat/config` | `{ "model", "timeout_s", "tools": ["Read","WebSearch","WebFetch","mcp__chronoquant"], "mcp_ok": bool }` (a UI fejléc/diagnosztika) |

### 2.11 reports — lásd §3

| Metódus + path | |
|---|---|
| `GET /api/reports?scope=model\|general&model_id=` | Manifest-**lista** (csak metaadat, tartalom nélkül) |
| `GET /api/reports/{owner}/{slug}` | `{ "manifest": {…}, "markdown": "<report.md szövege>" }` |
| `GET /api/reports/{owner}/{slug}/charts/{chart_id}` | Plotly figure JSON (`{data, layout, config?}`) |
| `GET /api/reports/{owner}/{slug}/tables/{table_id}` | Tábla JSON (§3.3) |

### 2.12 MCP mount

`/mcp/chronoquant` (nem `/api` alatt) — in-process FastMCP streamable-HTTP, a life_plan `_McpEndpoint` mintájára (route közvetlenül az app routerén, nincs 307-redirect; a session managert a lifespan indítja; az SPA-fallback **elé** regisztrálva).

---

## 3. Report-formátum

### 3.1 Chart-lib döntés: Plotly JSON (megerősítve)

Ellenőrzött tények:
- `analyst/lib/plot_utils.py` (a ticket `analyst/plot_utils` néven hivatkozza; a tényleges hely `analyst/lib/`) **matplotlib + seaborn**: `apply_theme()`, `timeseries/bar_monthly/kde_by_group/quarterly_boxplot/dual_axis/stacked_panels/hbar_features` mind `plt.*`/`sns.*`. A 4 notebook-sablon (`analyst/notebooks/01..04`) mind matplotlib-ot használ (`plotly`/`altair` egyik sem szerepel bennük).
- → Sem a Plotly, sem a Vega-Lite nem „átvehető": a chart-kódot **újra kell írni** (t884/t885) bárhogy. Ezért az újraírás célja a legkisebb költségű spec.
- `plotly>=5.24.0` már a `pyproject`-ben van, `src/ui/components/charts.py` 430 sor Plotly (gyertya + subplot + shape/annotation overlay), `plotly.io`/`fig.to_plotly_json()` triviális.
- Vega-Lite: új függőség (`altair`/`vl-convert`), a gyertya + 3-soros subplot + shape-overlay kényelmetlen; a hosszú idősorokon lassabb.

**Döntés: Plotly figure JSON.** Megkötések (builder kényszeríti):
1. Numpy tömbök **sima listává** (nem Plotly 6 `bdata` base64 typed array) — a JSON olvasható marad a chat/MCP számára és minden plotly.js verzióval megy: `json.loads(plotly.io.to_json(fig, engine="json"))`.
2. **Nincs beágyazott template/téma** a JSON-ban (`layout.template` törölve): a téma (világos/sötét, `LONG #198754`, `SHORT #dc3545`, `ACCENT #343a40`, `NEUTRAL #6c757d` — a `plot_utils` palettája) a frontend `PlotlyFigure` komponensben mergelődik (`reports/theme.py` ugyanezt adja a Python-oldali előnézethez).
3. Trace-enként max **5 000 pont** (a builder `max_points` + egyenletes/LTTB mintavétel); gyertyánál max 2 000.
4. A frontend `plotly.js-dist-min`-t **dinamikus importtal / `React.lazy`** tölti (külön chunk), csak a Reports/Strategy oldalon és a dashboard gyertya-grafikonján.

Élő dashboard grafikon: az API **adatot** ad (§2.2), a figure-t a kliens építi (interaktív ablakváltás, perces frissítés, overlay). Riportok: **kész figure JSON**.

### 3.2 Könyvtárszerkezet és scope

```
artifacts/<model_id>/reports/<slug>/        # scope = "model"
_doc_/reports/<slug>/                       # scope = "general"  (a _doc_/models_doc/ utódja)
    manifest.json
    report.md
    charts/<chart_id>.json
    tables/<table_id>.json
```

- `owner` az API-ban: `model_id` (scope=model) vagy a fix `general` (scope=general). A `general` foglalt, modell-id nem lehet (azok `lgbm_…` prefixűek).
- `slug`: `^[a-z0-9][a-z0-9_-]{0,63}$`. A sablonból jövő model-riportok slugjai fixek: `01_sampling`, `02_feature_engineering`, `03_hyperparameter_search`, `04_strategy` (megőrzi a régi számozást).
- A strategy-session riportja is a **modell** alatt él (a stratégia a modell-mappában van: `artifacts/<model_id>/strategy/`); `04_strategy` a long/short modell saját riportjába kerül.
- `reports/` almappa a modell-artifact gyökerében **nem keveredik** a model/strategy fájlokkal; a `_doc_/reports/` git-követett, az `artifacts/<id>/reports/` ugyanúgy követett, mint az eddigi `analysis/` (t890 `.gitignore`-t nem szigorít).
- Nincs notebook, nincs HTML, nincs Quarto.

### 3.3 Fájlformátumok

**`manifest.json`** (séma verzió: `1`, pydantic: `reports.schema.Manifest`):

```json
{
  "schema_version": 1,
  "id": "01_sampling",
  "title": "Mintavételezés (train/valid)",
  "scope": "model",
  "model_id": "lgbm_solusdt_l_fw60_2101_2607",
  "created_at": "2026-11-06T09:12:00Z",
  "updated_at": "2026-11-06T09:12:00Z",
  "order": 10,
  "summary": "1-2 mondatos kivonat a listához (opcionális)",
  "tags": ["sampling"],
  "source": { "kind": "pipeline_step", "step": "sample", "generator": "reports.builders.sampling", "git_sha": "abc1234" },
  "content": {
    "markdown": "report.md",
    "charts": [ { "id": "monthly_counts", "title": "Havi minták", "file": "charts/monthly_counts.json", "height": 420 } ],
    "tables": [ { "id": "fold_summary",  "title": "Fold összegzés", "file": "tables/fold_summary.json" } ]
  }
}
```

Kötelező: `schema_version, id, title, scope, created_at, order, content`. `model_id` kötelező ha `scope="model"`, `null` ha `general`. `id` == a mappa neve (slug). `order`: növekvő sorrend a listában (10-es lépések). Ismeretlen mezőt a reader tűri (előre-kompatibilis).

**`report.md`** — GFM markdown. Beágyazás **önálló sorú placeholderrel**: `{{chart:<chart_id>}}`, `{{table:<table_id>}}`. A frontend ezeket a helyükön renderelő komponensre cseréli (lazy: a figure/tábla csak akkor töltődik, ha a komponens a viewportba/megnyitott riportba kerül). Nem szereplő chart/table a riport végén listázódik. Nincs nyers HTML, nincs képfájl-hivatkozás.

**`charts/<id>.json`** — Plotly figure: `{ "data": [...], "layout": {...}, "config": {...}? }` (§3.1 megkötések).

**`tables/<id>.json`**:

```json
{ "columns": [ {"key":"fold","label":"Fold","type":"int"}, {"key":"mean_mfe","label":"Átlag MFE","type":"float","digits":4}, {"key":"hit","label":"Találati arány","type":"pct"}, {"key":"month","label":"Hónap","type":"datetime"} ],
  "rows": [ [1, 0.0123, 0.55, "2026-01-01T00:00:00Z"] ],
  "total_rows": 1, "truncated": false }
```
`type ∈ int|float|pct|str|datetime|bool`; `rows` oszlop-sorrendű tömbök; max **2 000 sor** (`truncated: true` + `total_rows`). A formázást (`pct`, `digits`) a frontend végzi (a `table_formatting` mintái: `_pct`/`_rate` oszlopok → pct).

### 3.4 Builder API (t884, `src/reports/builder.py`)

```python
rb = ReportBuilder(scope="model", model_id=..., slug="01_sampling", title="…", order=10)
rb.add_markdown("## Bevezető\n…")          # szöveg-blokk, sorrendben
rb.add_chart("monthly_counts", fig, title="…", height=420)   # plotly Figure -> charts/*.json + {{chart:…}} a markdownba
rb.add_table("fold_summary", df, title="…", columns={...})   # pandas/polars df -> tables/*.json + {{table:…}}
path = rb.write()      # atomikus: tmp mappába ír, majd rename; manifest utoljára; felülírja a meglévő slugot
```
Követelmény: determinisztikus kimenet (azonos input → azonos bájtok, kivéve `created_at/updated_at`), `write()` idempotens, a `pipeline.py --step analyze` ezt hívja a papermill/quarto helyett (t884 módosítja a lépést; ez a `modeling_agent` fájlja — a t884 csak az `analyze`/FE lépés hívásait cseréli, mást nem).

### 3.5 Lazy betöltés

1. Lista: `GET /api/reports` → csak manifest-ek (`content.charts/tables` listával, de **fájltartalom nélkül**). A reader a manifest-eket olvassa, a `report.md`-t/JSON-okat nem.
2. Kattintás egy riportra → `GET /api/reports/{owner}/{slug}` (manifest + markdown).
3. A markdownban lévő placeholder komponens csak mountoláskor kéri `…/charts/{id}` / `…/tables/{id}`; `Cache-Control: max-age=60` + `ETag` (fájl mtime hash).
4. A Plotly bundle a first chart-mountig nem töltődik.

### 3.6 Elemzések fül és MCP

Frontend: `ReportsPage` két szűrő (Modell: legördülő a `registry/models`-ből + „Általános"), bal oldali lista (order szerint), jobb oldali olvasó. A `GET /api/registry/models/{id}` is visszaadja az adott modell riport-listáját.
MCP: `list_reports(owner?)`, `read_report(owner, slug, include_tables=false)` (a markdown + a chart-ok címei; chart JSON-t csak kérésre).
A t885 a meglévő notebookokat/HTML-eket ebbe a formátumba konvertálja (modell-riportok: `artifacts/<id>/analysis/*.ipynb`; általános: `_doc_/models_doc/*.ipynb|md`, `archive/` nélkül — a t885 dönt, melyik érdemes átvinni).

---

## 4. Chat architektúra

### 4.1 Futtatás

- **Headless `claude -p`** a helyi, bejelentkezett CLI-vel (előfizetés, **nem** API-kulcs). `STRIPPED_ENV = (ANTHROPIC_API_KEY, ANTHROPIC_AUTH_TOKEN)` kiszűrve a gyerek env-ből.
- Portolt kód: `src/ui/chat/runner.py` (`resolve_claude_bin`, `build_command`, `build_env`, `map_cli_event`, `kill_tree`, `_spawn`, `_pump_stdout`, `stream_chat`) → `src/api/chat/runner.py`. A szinkron generátor `starlette.concurrency.iterate_in_threadpool`-lal megy az SSE `StreamingResponse`-ba; **kliens-szakadáskor** a generátor `close()`-a a `finally`-ban kilövi a teljes process-fát (`kill_tree`). A `model.py` (Streamlit UI-state) **nem** költözik — a UI-állapot a frontendé (`ChatPanel` + `localStorage`).
- Parancs (változatlan elvek): `-p --output-format stream-json --verbose --include-partial-messages --permission-mode dontAsk --disable-slash-commands --setting-sources "" --strict-mcp-config --model <cfg> --settings '{"showThinkingSummaries":true,"alwaysThinkingEnabled":true}' --append-system-prompt <persona>`; az üzenet **stdin-en** megy (Windows parancssor-limit).
- cwd: repo gyökér. Konfig: `utils.load_chat_config()` (`model` default `sonnet`, `timeout_s` 1800, `persona_prompt_path` → **új default `src/api/chat/persona_prompt.md`**; `allowed_bash` kulcs megszűnik).

### 4.2 SSE események (kliens↔szerver kontraktus)

```
event: session   data: {"id": "<claude session uuid>"}
event: delta     data: {"text": "<szövegdarab>"}
event: thinking  data: {"text": "…", "tokens": 123}          (text lehet üres)
event: tool      data: {"name": "mcp__chronoquant__get_candles", "status": "start|done|error", "id": "toolu_…", "input": "<rövid ember-olvasható>"?}
event: done      data: {"session_id": "…", "duration_ms": 0, "cost_usd": 0.0}
event: error     data: {"message": "…", "code": "binary_not_found|spawn_failed|timeout|cli_exit|error"}
```
Formátum: `event: <név>\ndata: <egysoros JSON>\n\n`. Kliens: `fetch` + `ReadableStream` (EventSource nem tud POST-ot) — `frontend/src/lib/sse.ts` a life_plan `chatStream.ts` mintája (`createSseParser`, `splitToolName`, `isWriteTool` a `mcp__chronoquant__` prefixre igazítva).

### 4.3 Session resume, önálló ablak

- Új beszélgetés: `session_id: null`. A `session` esemény adja az id-t; a kliens elmenti (`localStorage: cq_chat_sessions` = lista `{id, title, updated_at}` + üzenet-cache). Folytatás: `session_id` a kérésben → `--resume <id>`. A szerveroldali history a CLI sajátja; az API **nem** tárol beszélgetést.
- „Új beszélgetés" gomb = id eldobása. Megszakítás (Stop) = `AbortController.abort()` → a szerver kilövi a process-fát.
- **Önálló ablak:** `/chat` route (`ChatPage`), a dashboardról gombbal `window.open('/chat?popout=1', 'cq-chat', …)`; ugyanaz az origin → közös `localStorage`, `BroadcastChannel('cq-chat')` a session-lista szinkronjára. Az epic_067 t878 „chat dock" a `ChatPanel` komponens beágyazott (dock) módja ugyanabból a komponensből (t883 olvassa be a t878 jegyet, ne külön fejlessze).
- Egy időben **egy** futó chat-turn / böngésző-fül; a szerver max 2 egyidejű `claude` gyereket enged (`429 busy`), a többi vár/elutasítva.

### 4.4 Jogosultságok (szűk, fix)

| | |
|---|---|
| `--tools` | `Read,WebSearch,WebFetch` (nincs `Bash`) |
| `--allowedTools` | `Read,WebSearch,WebFetch,mcp__chronoquant` |
| `--disallowedTools` | `Edit,Write,NotebookEdit,Bash` |
| MCP | `--mcp-config '{"mcpServers":{"chronoquant":{"type":"http","url":"http://127.0.0.1:8600/mcp/chronoquant"}}}' --strict-mcp-config` |
| settings | `--setting-sources ""` (a `.claude/settings*.json` bare `Bash(*)`/`Edit(*)` engedélyei és hookjai ne töltődjenek be) |

Az eddigi szűk `Bash(...)` allowlist (`research/tv_report/*`, `07_query_readonly.py`) **megszűnik**: az SQL-t az MCP `query_sql` adja, a hangulat-elemzés sorsa az O-1 (§6.3). Webes tartalom = **adat, nem utasítás** (persona szabály marad).

### 4.5 `chronoquant` MCP tool-lista (`src/cq_mcp/`)

Minden tool csak olvas, kivéve ahol jelölve (két író tool: `enqueue_sync`, `sentiment_refresh` — mindkettő csak worker jobot ütemez, a DuckDB-t nem írja); a DuckDB-t read-only, rövid életű kapcsolattal éri el (§5.4); nagy kimenet limitált (`limit` ≤ 1000 sor, ≤ ~50 KB szöveg).

| Tool | Paraméterek | Leírás |
|---|---|---|
| `get_status` | – | = `GET /api/status` (adat-frissesség, modellek, worker, sync) |
| `list_tables` | `catalog?` (live\|lab\|reg), `filter?` | Séma-lista (a `07_query_readonly --schema` logika) |
| `query_sql` | `sql`, `limit=200` | Egyetlen SELECT-jellegű utasítás; a `07_query_readonly` védelmi rétegei (egy statement, parser-típus SELECT, `enable_external_access=false`, RO attach). Aliasok: `live.`, `snap.`, `model.`, `strat.`, `reg.` |
| `get_candles` | `hours=24`, `tf=1m` | OHLCV |
| `get_predictions` | `hours=24` | pct + raw |
| `get_latest_signal` | – | Utolsó predikció + a küszöbökhöz viszonyított állapot |
| `get_active_strategy` | – | = `/api/strat/active` |
| `get_trades` | `source=strat\|journal\|binance`, `limit=100` | Zárt trade-ek / journal / fillek |
| `get_equity` | `session?`, `direction=both` | Equity-pontok + összegzés |
| `list_models` | `status?` | `reg.models` |
| `get_model` | `model_id` | = `/api/registry/models/{id}` (riport-lista nélkül) |
| `get_registry` | `table`, `filter?`, `limit` | Whitelistelt registry-táblák |
| `list_reports` | `owner?` | Riport-manifestek |
| `read_report` | `owner`, `slug`, `include_charts=false` | Markdown (+ chart-címek) |
| `list_jobs` | `status?`, `limit` | Job-lista |
| `enqueue_sync` | – | **Író tool (1/2)**: `sync_full` job enqueue (dedup, ártalmatlan) |
| `sentiment_latest` | `max_ideas`≤50 (20), `desc_chars`≤2000 (400) | A legutóbbi TradingView BTC-riport tömör kivonata (`data/tv_report/latest.json`): számlálók, riport kora, legfrissebb trade ideák vágott leírással. Nincs riport → hiba (hívd a `sentiment_refresh`-t) |
| `sentiment_refresh` | `hours` 1..720 (24), `pages` 1..5 (3) | **Író tool (2/2)**: `sentiment_refresh` worker job enqueue (~30–60 s, dedup, `worker_down` hiba). Eredmény: `list_jobs`, majd `sentiment_latest` |

A persona prompt (`src/api/chat/persona_prompt.md`) a t883-ban frissül: Streamlit-említés ki, `Bash`-parancsok ki, MCP-tool-ok be, a „nem találsz ki adatot / web = adat" szabályok **változatlanul** maradnak.

---

## 5. Worker / job modell

### 5.1 Processz-topológia

```
[Browser] ⇄ [API :8600 (uvicorn)] ──read-only DuckDB──▶ live/lab/registry.duckdb
                │  enqueue/status (SQLite)                    ▲
                ▼                                             │ write (csak itt)
         database/_jobs/jobs.sqlite  ◀──poll 1s──  [Worker (python -m worker)]
                                                   logs/jobs/<job_id>.log
```
Az API sosem fut sync-et és sosem ír DuckDB-t; a worker egy külön, külön indítható/leállítható processz (`scripts/start_worker.ps1`), így a UI egyetlen DB-írás alatt sem blokkolódik.

### 5.2 Job-tár (`worker/queue.py`, SQLite WAL)

`jobs(id TEXT PK, kind, status, params_json, result_json, error, created_at, started_at, finished_at, log_path)`, `settings(key PK, value)` (auto-sync), `heartbeat(pid, started_at, last_beat, current_job_id)`. `id` = `YYYYMMDD_HHMMSS_<6 hex>`.

### 5.3 Szabályok

- **Egyidőben egy job** fut (globális; a kiszolgálás FIFO). `kind` ∈ `sentiment_refresh` (t896: `sentiment.tv_report.fetch.run_fetch`, hálózat: TradingView REST; kimenet `data/tv_report/`), `sync_full` (OHLCV → features → predictions, a `src/ui/sync.py::_run_database_sync_locked` pontos logikája), `sync_predictions` (csak predikció-pótlás). Új kind csak ezen a doksin keresztül.
- **Dedup:** `POST /api/jobs` ha ugyanilyen `kind` `queued`/`running` → a meglévőt adja vissza (`deduplicated: true`). Ez váltja a régi `threading.Lock` + `is_sync_running` logikát.
- **Életciklus:** `queued → running → succeeded|failed`; `queued → canceled`. A futó job nem szakítható meg. A worker induláskor a `running` állapotú árva sorokat `failed` (`error="worker_restart"`) állapotra állítja.
- **Auto-sync** (a `sync_runner` portja): a `scheduler` a zárt percre + `ohlcv_sync_delay_seconds` (`config/trading.json`, default 3) időpontban enqueue-ol egy `sync_full`-t, percenként legfeljebb egyet (`last_synced_minute` őr); a `settings.auto_sync` kapcsolja, `PUT /api/jobs/auto-sync`. Alapból **ki** van kapcsolva.
- **Heartbeat** 5 mp-enként; `alive` = az utolsó beat < 15 mp. API: `worker_down` ha nem él és enqueue érkezik.
- **Log:** a worker a handler `stdout`/`logging` kimenetét a `logs/jobs/<id>.log`-ba írja (a `_LoggerWriter` redirect minta marad); az SSE/`log` végpont a fájl byte-offsetét követi (500 ms poll). Retenció: 200 job / 30 nap, a worker induláskor takarít.
- **Live trading vezérlés NINCS** (sem start/stop, sem mód-váltás): a `trading_runner.py` funkciói nem kerülnek át.

### 5.4 DuckDB-zárolás (kritikus kockázat)

DuckDB-ben egy fájlt, amit egy processz **írásra** nyitott, másik processz **nem** nyithat meg (read-only módban sem). A régi UI ezt `duckdb.IOException: …locked…` elkapásával kerülte meg. Kontraktus:
1. A **worker** a `live.duckdb`-t írásra csak a ténylegesen író szakaszokra nyitja (a `sync_*` függvények a mostani módon, kapcsolat-per-művelet); a handler nem tart nyitva kapcsolatot a lépések között.
2. Az **API** minden kérésre **rövid életű `read_only=True`** kapcsolatot nyit (a `07_query_readonly` mintájára: in-memory kapcsolat + `ATTACH … (READ_ONLY)`), `with`/`finally` lezárással; zárolási hiba esetén 3× újrapróbál 200 ms backoff-fal, utána `503 db_busy`.
3. A frontend `db_busy`-nál **megtartja az utolsó adatot** és halvány „szinkron folyamatban" jelzést mutat (a régi `cache_hist` viselkedés).
4. `GET /api/status` → `db.state`: `busy` ha az utolsó kapcsolódás zárolásba futott.
Ha a mérés (t880/t881) azt mutatja, hogy a sync túl hosszan tartja a zárat, a worker lépésenként (ohlcv / features / predictions) külön commitolja a kapcsolatot — ez a t881 hatásköre, a kontraktus nem változik.

### 5.5 Scriptek (`scripts/`, PowerShell, a life_plan mintájára)

`_common.ps1` (Root, Port 8600, Python = `.venv\Scripts\python.exe`, `Test-ApiHealthy` → `/api/health`, `Get-ApiListenerPid`, `Stop-*`), `start_chronoquant.ps1` (rejtett `python -m uvicorn api.main:app --app-dir src --host 127.0.0.1 --port 8600`; health-várakozás 90 mp; Chrome-fül; `-NoBrowser`), `stop_chronoquant.ps1`, `start_worker.ps1`/`stop_worker.ps1` (rejtett `python -m worker`, pid-fájl `logs/worker.pid`), `dev.ps1` (uvicorn `--reload` + `npm run dev` + worker), `build_frontend.ps1` (`npm ci && npm run build`), `install_autostart.ps1` (Task Scheduler: API + worker; a meglévő `ChronoQuantUiAutostart` taskot a t891 cseréli). A `start_ui/stop_ui` + régi `_common.ps1` a t889/t891-ben törlődik.

---

## 6. Törlési lista (csak LISTA — semmi nem lett törölve)

Ellenőrzés: `rg` (kód/config/.agent/_doc_ hivatkozások, `_jira_/` kizárva), `git ls-files`, a registry (`database/_registry/registry.duckdb`). A „feltétel" oszlop a végrehajtási sorrendet adja; **t890 csak akkor töröl, ha a feltétel teljesült**, és minden törlés után lefuttatja a `registry_validator`-t.

### 6.1 Gyökér és dokumentáció (t890)

| Út | Állapot / bizonyíték | Feltétel |
|---|---|---|
| `_chronoquant_docs.ipynb`, `_chronoquant_docs_files/` (564 KB) | Csak az `analyst/doc_renderer/*` hivatkozza | Az `analyst/` törlésével együtt |
| `PLAN_tv_market_sentiment.md` | **0 hivatkozás** (kódban, configban, .agent-ben) | Törölhető; t890 előbb futtasson rá egy gyors átfutást — ha az indoklása egyedi, 5–10 soros kivonat → `_doc_/research/tradingview_author_skill.md`-be |
| `control.ipynb` (tracked) | **0 hivatkozás** | Törölhető |
| `trading_reports/` (tracked, 1 `summary.json`) | Írója: `src/trading/live/service.py:477` + `config/trading.json → journal.report_dir` | **t889 döntése** (src/trading sorsa). Ha a service marad: a tartalom törölhető + `.gitignore`-ba; ha a service megy: a config-kulccsal együtt |
| `docs/` (35 tracked md; architecture, business, concepts, data, engineering, evaluation, modeling, reference) | Kódból/configból **0** hivatkozás; csak `.agent/skills/methodology_doc_skill.md:53` és `.agent/agents/methodology_agent.md:110` említi „legacy forrásként" | **t891 előbb** kiveszi a két hivatkozást; utána törölhető. (A `_doc_/models_doc/*.html` szövegében szereplő „docs/" csak legenerált tartalom) |
| `tools/` (15 MB, **teljes egészében gitignored**: aria2, reddit-dump torrent/ingest scriptek, logok) | `src/`, `config/`, `scripts/`-ből **0** hivatkozás; csak szöveges említés a `_doc_/research/reddit_attention_gate.md`-ben | Lemezről törölhető (git-hatás nincs); a `.gitignore` `tools/` sort a t890 ezzel együtt elhagyhatja |
| `.gitignore` | `tools/`, esetleges új sorok (`database/_jobs/`, `frontend/node_modules/`, `frontend/dist/`) | t890/t880 |

### 6.2 `analyst/` és Quarto (t884 → t885 → t890)

Állapot: `src/` egyetlen kapcsolata: `src/modeling/pipeline.py` (`ANALYSIS_TEMPLATES_DIR = analyst/notebooks`, papermill + nbformat + `uv run quarto render`) és `src/modeling/01_feature_engineering.ipynb|.html` (az FE-lépés papermill sablonja). A `analyst/lib/db_utils.py` / `table_formatting.py` / `plot_utils.py`-t **csak** az `analyst/` notebookok használják (`src/*.py`-ból `import analyst…` **nincs**).

| Út | Feltétel |
|---|---|
| `analyst/notebooks/` (4 ipynb + `__init__.py`), `analyst/quarto/` (`_quarto.yml`, css), `analyst/doc_renderer/` (3 fájl), `analyst/lib/` (`plot_utils.py`, `table_formatting.py`, `db_utils.py`), `analyst/__init__.py` → **az egész `analyst/`** | **t884** átírta a `pipeline.py` analyze+FE lépést a `reports` builderre **és t885** konvertálta a riportokat. A paletta (`plot_utils` színkonstansai) már átment a `src/reports/theme.py`-ba; a `db_utils` kellő függvényei a `reports/builders/*`-ba |
| `src/modeling/01_feature_engineering.ipynb`, `…/01_feature_engineering.html` (tracked) | t884 (az FE lépés builderre cserélve) — a törlést a t884 végzi/ellenőrzi, t890 csak verifikál |
| `.agent/tools/quarto_analysis_defaults.md`, `.agent/skills/analyst_skill.md` (átírás), `_doc_/database_and_code_doc/8200_analyst_templates.md` (törölve; helyette `8330_reports.md`) | t891 / t884 |
| `.agent/agents/analyst_agent.md` hatásköre (Quarto → report builder) | t891 |

### 6.3 `research/` (t886) és az O-1 nyitott kérdés

`research/` tartalma (mind kimenet/kísérlet; `src/`-ből csak alább jelölt hivatkozások):

| Mappa | Hivatkozás | t886 teendő |
|---|---|---|
| `research/cluster_regime/` (7 .py + `__init__`) | `_doc_/models_doc/65xx*.md`, `methodology_doc/6500_*` szöveges említés; kimenete: `artifacts/research_cluster_regime/` | rövid md → `_doc_/research/cluster_regime.md`, törlés |
| `research/ma_cross/` (README, 2 .py, `out/`) | nincs | md → `_doc_/research/ma_cross.md`, törlés |
| `research/rsi_divergence/` (1 .py, `out/`) | nincs | md → `_doc_/research/rsi_divergence.md`, törlés |
| `research/tradingview_poc/` (README, REVIEW, probe*.py, out, samples) | csak kommentben: `src/data_handling/collectors/tradingview/api.py:9` („see research/tradingview_poc/README.md") | md → `_doc_/research/tradingview_poc.md`; a kommentet t887 frissíti |
| `research/tv_report/` (`fetch_report_data.py`, `author_backtest.py`, ~50 `scratch_*.txt`, `*_latest*.json`, `state.json`, `runs/`) | **ÉLŐ FÜGGŐSÉG:** `src/utils.py:385` `_CHAT_DEFAULTS["allowed_bash"]`, `.agent/skills/tv_sentiment_skill.md`, `src/ui/chat/persona_prompt.md`; a scriptek importálják a `data_handling.collectors.tradingview.*`-t | **BLOKKOLT — O-1** |

**O-1 (user-döntés kell):** a chat „piaci összkép (TradingView ideas)" képessége a `research/tv_report/` két scriptjén fut. A research/ teljes törlése ezt megszüntetné. Javaslat (alapértelmezés, ha a user nem mondja mást): a két **működő** script (`fetch_report_data.py`, `author_backtest.py`) költözik `src/sentiment/tv_report/`-be (a `collectors.tradingview` mellé; kimenet `data/tv_report/`, gitignored), és MCP tool-ként él: `sentiment_latest` (a `latest.json` tömör olvasása) + `sentiment_refresh` (worker-job `tv_report_fetch`, mert fél–egy percig tart). A `scratch_*.txt`, `*_latest*.json`, `state.json`, `runs/` törölhető. Ha a user a képességet **elengedi**: a persona prompt/`tv_sentiment_skill.md`/`allowed_bash` tisztítása a t883/t891-ben, a `research/tv_report/` a többivel megy. **t886 a `research/tv_report/`-hoz csak az O-1 eldőlte után nyúlhat.** (Felelős: t883 költöztet/dönt-implementál, t886 törli a maradékot.)

### 6.4 `artifacts/` (t885 után, t890)

| Út | Bizonyíték | Feltétel |
|---|---|---|
| `artifacts/<model_id>/{01_sampling,02_feature_engineering,03_hyperparameter_search,04_strategy}.html` (×2 modell) | renderelt Quarto riportok | t885 konvertálta |
| `artifacts/<model_id>/analysis/*.ipynb` (4 ×2 modell) | notebook-források | t885 konvertálta |
| `artifacts/<model_id>/feature_engineering/01_feature_engineering.{ipynb,html}` (×2) | **`feature_set.json` marad** (reg.feature_sets link) | t884/t885 |
| `artifacts/_experimental/` / `search/_experimental/` | a project overview említi, de a jelenlegi fában **nem létezik** (`search/` alatt: `best_params.json, optuna_study.db, search_*.json, trial_curves, trial_logs`) | t890 `find artifacts -name _experimental` ellenőrzés; ha nincs, nincs teendő |
| `artifacts/oos_champ_A`, `oos_champ_B`, `wf_champ_{1,2,3}_{sel,test}`, `strat_champion_long_netcheck` (kísérleti stratégia-artifactok) | `src/`, `config/`-ban **0** hivatkozás (az aktív stratégia: `config/trading.json → <model_id>/strategy`); **de** a registry hivatkozik rájuk (`reg.artifacts`: 90 sor, `reg.strategies`: 9 sor, az `oos_champ|wf_champ|netcheck` mintára) | Törölhető **csak** a hozzájuk tartozó `reg.artifacts`/`reg.strategies` sorok kezelésével (t890 SQL: előbb listáz, user-jóváhagyás, majd törlés/`status='archived'`); a `lab.duckdb` `strat.*` táblák maradnak (külön döntés) |
| `artifacts/strat_solusdt_fw60_{long,short}_2101_2607/` | `src/`, `config/`: 0 hivatkozás; `reg.artifacts`: 0 sor; az aktív stratégia a modell-mappában van | Törölhető (t890, duplikátum-ellenőrzés a `<model_id>/strategy/` tartalmával) |
| `artifacts/research_cluster_regime/` | a `research/cluster_regime` kimenete | t886-tal együtt |
| **Registry-higiénia:** `reg.artifacts` 18 sor `.html/.ipynb` útvonallal (`fe_html` 9, `fe_notebook` 9) | törlés után dangling | t890 a fájltörlés után törli/archiválja ezeket a sorokat, `registry_validator` zöld |

### 6.5 Python-kód: `src/ui/` és kapcsolódók (t889/t891)

`src/ui/` **teljes egészében** (main.py, data.py, binance_data.py, sync.py, sync_runner.py, trading_runner.py, dashboard_logging.py, components/*, chat/*, `.streamlit/`, tests/) — **de csak a t880–t883 + t882 portjai után**. Port-térkép: `data.py`/`binance_data.py` → `api/services/*`; `sync.py`/`sync_runner.py` → `worker/*`; `chat/runner.py` + persona → `api/chat/*`; `components/charts.py` → `frontend/…/PredictionPriceChart.tsx`; `trade_panel`/`log_panel`/`main.py` kártyák → dashboard komponensek; `trading_runner.py` → **nincs** (D-7). Az ui-teszteket (`tests/chat/smoke`) a t883 portolja az `api/tests/`-be.
`src/trading/` sorsa: t889 javasol, a user dönt (D-7: vezérlés kikerül; a journal-táblák olvasása marad a `trade journal` nézethez).
`scripts/start_ui.ps1`, `stop_ui.ps1`, régi `_common.ps1`, a `ChronoQuantUiAutostart` task → új scriptek (§5.5) után; `logs/ui.*.log`, `logs/streamlit_dashboard.log` (gitignored `*.log`) törölhető.

### 6.6 Függőségek (`pyproject.toml`, `uv remove` — t890)

Ellenőrzött import-használat (`^\s*(import|from) <pkg>` a `src/ research/ analyst/ scripts/` `.py`-jain):

| Csomag | Hol használt | Döntés |
|---|---|---|
| `streamlit` | csak `src/ui/*` (+ `ui/tests`) | **töröl** t889 után |
| `papermill` | csak `src/modeling/pipeline.py` | **töröl** t884 után |
| `jupyter`, `nbconvert` (dev) | **0 import**; `nbformat` nincs közvetlen dep (tranzitív) | **töröl** (t884 + t885 után; a riport-konverziós egyszeri script futása idejére maradhat) |
| `seaborn` | csak `analyst/lib/plot_utils.py` | **töröl** az `analyst/` után |
| `mplfinance`, `taew` | **0 import** a repóban | **töröl** (feltétel: `rg "mplfinance|taew"` üres a törlés pillanatában) |
| `matplotlib` | `analyst/lib/plot_utils.py` + `src/modeling/training/reports.py` | **t888 dönt** (a `training/reports.py` megmarad-e); az `analyst/` miatt önmagában nem tölthető |
| `statsmodels` | csak `src/modeling/experiments/*` (7 fájl) | **t888 dönt** (experiments audit) |
| `plotly` | `src/ui/components/charts.py` + **az új `reports` builder** | **marad** (a builder használja) |
| `tabulate` | nincs közvetlen import, de a `pandas.to_markdown` (pl. `07_query_readonly --format markdown`) igényelheti | **marad**, amíg a markdown-kimenet él |
| `zstandard` | `collectors/reddit/dump_ingest.py` | t887 dönt a reddit collectoról |
| `pyinstaller` (dev) | 0 hivatkozás | **töröl** (t890 ellenőrzi a `.spec`/script hiányát) |
| **ÚJ:** `fastapi`, `uvicorn[standard]`, `mcp` | API/MCP | t880/t883 `uv add` |

Minden `uv remove` után: `uv sync`, `uv run pytest` a megmaradó modulokra, `ruff`, `pyright`.

### 6.7 Nem a törlési lista része, de t887/t888 audit-tárgy (megjegyzés)

`src/modeling/analyst_signal/` + `data/analyst_signal` + `store/analyst_signal_store.py` + `collectors/reddit|derivatives|universe` (altdata/research jellegű); `src/modeling/experiments/` (25 fájl); `src/data_handling/collectors/tradingview/review.py|backfill.py` — az auditok döntenek, itt csak jelezve.

---

## 7. Hibrid függőség-térkép

Elv: az API/worker/MCP **vékony**, a meglévő, tesztelt adatréteget használja újra; a `src/utils.py` marad az egyetlen config-kapu (nincs nyers JSON-olvasás).

| Réteg | Importálhat | Nem importálhat |
|---|---|---|
| `api` (routers/services) | `utils` (`load_*_config`, `open_lab_connection`, `repo_root`, `live_model_id`, `apply_rank_percentile`, `next_open_time`…), `data_handling.store.duckdb_query` (`query_range`, `latest_open_time`, `row_count`, `dataset_exists`, `dataset_columns`), `data_handling.store.registry` (olvasó függvények; ha nincs, raw RO SQL), `reports.store`/`reports.schema`, `cq_mcp` (csak a mount) | `modeling.*`, `strategy.*`, `trading.*`, `ui.*`, bármi, ami írási DuckDB-kapcsolatot nyit |
| `worker` | `utils`, `data_handling.sync_tables.{sync_ohlcv,sync_features,sync_predictions}`, `data_handling.store.duckdb_query` (`ohlcv_row_count`, `ohlcv_latest_open_time`, `latest_open_time`) | `api.*`, `ui.*`, `trading.*` |
| `cq_mcp` | `api.services.*` (ugyanazok a service-függvények, hogy a REST és az MCP ugyanazt adja), `reports.store` | közvetlen DuckDB-nyitás a service-rétegen kívül |
| `reports` | `plotly`, `pandas`/`polars`, stdlib; a `builders/*` olvashat: `utils`, `data_handling.store.*` (RO), `modeling.*`/`strategy.*` **csak** a `builders/`-ben és csak ha nincs más út (a builderek a pipeline-ból hívódnak, nem az API-ból) | az `api`-ból nem hívható a `builders/` |
| `frontend` | csak HTTP `/api`, `/mcp` | – |

Kötelező új helper (t880, minimális, mozgatás-jellegű refaktor): a `src/data_handling/07_query_readonly.py` védelmi logikáját (statement-ellenőrzés, RO attach, `lock_configuration`) **modulba** kell emelni (`src/data_handling/store/readonly_query.py`: `run_readonly_query(sql, limit)` + `describe_schema(...)`), a CLI marad vékony wrapper — ugyanezt hívja az MCP `query_sql`. (A `07_…` név nem importálható modulnévként; a viselkedés nem változhat — a meglévő tesztjei zöldek maradnak. Érintett agent: `database_agent`-hatáskör, a t880 csak a mozgatást végzi, t887 audit-jegyzetébe kerül.)
Kiszervezés a `ui/data.py`-ból: a `_load_session_artifact`, `_session_artifact_path`, `_load_rank_lookups`, `_load_strat_table`, `closed_trades`, `equity_curve`, `latest_prediction`, `prediction_history`, `active_position`, `table_health` → `api/services/*` (pandas-ból JSON-ba, `NaN→null`); a `pandas` az API-ban megengedett.

---

## 8. Task → szakasz térkép és sorrend

| Task | Agent | Kontraktus-szakasz | Megjegyzés |
|---|---|---|---|
| t880 BFF váz + read endpointok | ui_agent | §1, §2.0–2.7, 2.9, §5.4, §7 | + `readonly_query` kiemelés |
| t881 Worker/job-manager | ui_agent | §5, §2.1 (stream), §2.8 | a `/api/jobs*` router implementációja is itt |
| t882 Frontend váz + dashboard | ui_agent | §1, §2.2–2.8, §3.1 (chart-portolás) | `PredictionPriceChart` a t889 előtt |
| t883 Chat + MCP | ui_agent | §4, §2.10, §2.12, §6.3 (O-1) | `src/cq_mcp/` (D-2) |
| t884 Report builder + `/api/reports` + Elemzések fül | analyst_agent | §3, §2.11 | `pipeline.py` analyze/FE lépés átírása |
| t885 Riportok konvertálása | analyst_agent | §3, §6.4 | |
| t886 research/ → md | analyst_agent | §6.3 | `tv_report` blokkolt O-1-re |
| t887 / t888 audit | database_agent / modeling_agent | §6.7, §7 | |
| t889 trading + ui kivezetés | ui_agent | §6.5 | a user dönt a `src/trading`-ról |
| t890 repo takarítás | code_doc_agent | §6 | a „Feltétel" oszlopok szerint |
| t891 overview, manifestek, scriptek | code_doc_agent | §1, §5.5 | `0000_project_overview.md` + `.agent/*` + `_doc_/database_and_code_doc/80xx_ui*.md` újraírása |
| t892 validálás | validator_agent | – | ruff + pyright + pytest + `registry_validator` + frontend `tsc`/build |

Függőségi sorrend: **t879 → {t880, t881, t884, t886} → t882 (←t880), t883 (←t880,t881) → t885 (←t884) → t887/t888 → t889 (←t882, t883) → t890 → t891 → t892.** (A t886 `tv_report` részéhez O-1 kell.)

## 9. Nyitott pontok / feltevések

- **O-1** — `research/tv_report/` sorsa (lásd §6.3). Default: költözik `src/sentiment/tv_report/`-be + MCP tool.
- **O-2** — `src/trading/` sorsa: t889 javaslata után user-döntés (D-7 szerint csak a journal olvasás biztos).
- **O-3** — a `_doc_/reports/` mint az általános riportok helye (a ticket `reports/<scope>/<slug>/` formáját a `{artifacts/<model_id>/reports, _doc_/reports}` kettős gyökér konkretizálja; ha a user külön top-level `reports/` mappát szeretne, csak a `reports.store` gyökér-feloldása változik).
- **A1** — feltevés: single-asset (`solusdt`), egyfelhasználós localhost, nincs autentikáció; az API csak `127.0.0.1`-re bindol.
- **A2** — feltevés: a `claude` CLI elérhető a PATH-on / a `resolve_claude_bin` keresési helyein (ugyanaz, mint a mai chatnél).
- **A3** — a ticket `analyst/plot_utils` hivatkozása pontosítva: `analyst/lib/plot_utils.py`.
