---
epic: epic_074
id: t942
title: `sync_scout` worker job + API végpont
assignee: ui_agent
status: done
blocks: [t943]
blocked_by: [t941]
---

## Goal
Worker job `sync_scout` (asset paraméterrel), az auto-sync ütemező a `sync_full` után futtatja SOL/BTC/ETH-re. API: `GET /api/scout/signals?asset=&since=` és `GET /api/scout/latest?asset=` (read-only), MCP tool opcionális. Szabályok: a worker-t ne állítsd le; az API csak read-only DuckDB; minden írás worker job. Holdout-2 (≥ 2026-08-01) adat nem kerülhet modell-illesztésbe.

## Scope
`src/worker/`, `src/api/routers/`, `src/cq_mcp/` (opcionális), tesztek

## Acceptance Criteria
- [x] Job lefut mindhárom assetre, hibatűrő DB-lockra
- [x] API végpontok + tesztek
- [x] Doksi: 8300, 8310

## Notes

**2026-11-23 (ui_agent) — kész, validálásra vár.** (Nem commitolva; worker/API nem lett újraindítva.)

### Worker
- `src/worker/assets.py`: Scout helperek — `scout_config()` (opcionális `config/trading.json` `scout` blokk: `model_id`,
  `assets`, `min_interval_s`; a config fájl NEM változott), `scout_model_id()` (alap `scout_s0c_r1`), `scout_manifest()`,
  `scout_asset_ids()` = override, különben `manifest.train.assets` (SOL/BTC/ETH) ∩ konfigurált assetek → **BCH kimarad**.
- `src/worker/scout_runner.py` (új): `get_artifact()` lusta, folyamatonként egyszer (~0,75 s, hash-ellenőrzéssel);
  `run_scout(asset)` → `sync_scout(asset, art=..., attempts=3, wait_s=1)`; nem Scout asset = `NotScoutAssetError`
  (különben `ensure_tables` létrehozná a táblát BCH-n). `maybe_run_scout(asset, new_bars)` = auto-sync lépés, sosem dob.
  Riasztás: `immutable_mismatch>0` / `missing_in_run>0` → ERROR `SCOUT ALERT <asset>` + `alert: true`.
- `handlers/sync_full.py`: OHLCV + funding után Scout lépés, csak ha Scout asset ÉS új 1m bár ÉS az asset utolsó próbálkozása óta
  ≥ `min_interval_s` (alap 300 s; hibánál is rögzül). Payload új kulcs: `scout` (`ok`+számlálók / `skipped`+`reason` / `failed`+`error`).
  CPU: ~0,7 s/asset → ~2 s / 5 perc a 3 assetre; a throttle csak az írási késést növeli (max ~6 perc ≪ H = 8 h).
- Önálló job: `sync_scout` (`JOB_KINDS` + `ASSET_KINDS` → dedup `(kind, asset_id)`), `handlers/sync_scout.py`, throttle nélkül.
- `api/routers/jobs.py`: `JobCreate.kind` + `sync_scout`; nem Scout asset → 400 `bad_request`, ismeretlen → 404.

### API (read-only, `src/api/routers/scout.py`, `services/scout.py`, `schemas.py` Scout* modellek)
- `GET /api/scout/signals?asset=&since=&limit=` (limit 1..2000, alap 500) → `{asset_id, available, items: ScoutEvent[]}`
  — a legújabb `limit` esemény `t0 >= since`, t0 szerint **növekvő**.
- `GET /api/scout/latest?asset=&recent=` (recent 1..50, alap 5) → `{asset_id, available, meta, counts, open_events, recent}`
  (`recent` újak elöl).
- `ScoutEvent` = tábla sor, de a szabály-oszlopok `rules: {default|r2_03: {tp, sl, outcome, outcome_at, exit_price, net_bps}}`
  alatt + `write_lag_s`. `meta` a manifest JSON-ból (a `scout` csomag nincs importálva): `model_id, display_name, status,
  experimental, scout_asset, thresholds{top20,top10}, live_start, detect_anchor, holdout2_start, cell_id, h_min, train_assets,
  oof_auc, rules[{key,name,description}], freeze_hash`.
- Nincs tábla (BCH): 200, `available=false`, üres listák, `counts=null`; ismeretlen asset: 404.

### MCP
- `scout_signals(asset, since, limit)` (`src/cq_mcp/tools_scout.py`, olvasó; tool-lista 19 → 20).
  Megjegyzés: `.agent/agents/ui_agent.md` még „19 tool”-t ír — code_doc_agent terület, nem módosítottam.

### Frontend (t943-hoz, UI komponens nincs)
- `frontend/src/types/api.ts`: `ScoutEvent`, `ScoutRuleResult`, `ScoutSignals`, `ScoutCounts`, `ScoutMeta`, `ScoutLatest` (+ status/outcome/label típusok).
- `frontend/src/api/scout.ts` (új): `getScoutSignals(asset, {since, limit})`, `getScoutLatest(asset, recent)`, `enqueueScoutSync(asset)`.

### Ellenőrzés
- `uv run pytest src/api src/worker src/cq_mcp -q`: **119 passed** (új: `src/worker/tests/test_scout_runner.py` 13,
  `src/api/tests/test_scout_api.py` 5 — valódi v8 DDL, `test_tools.py::test_scout_signals`). ruff + pyright (érintett fájlok): tiszta; `npx tsc --noEmit`: tiszta.
- Élő DB-k read-only (TestClient, új app): SOL 8 sor / 1 nyitott (`20261123T0242`), BTC 9, ETH 8, BCH `available=false`;
  SOL `20261121T0617` P(imp) 0,0964 (= t941) ✔; ismeretlen asset 404 ✔; `scout_asset_ids()` = SOL/BTC/ETH ✔.
  A `sync_scout` író utat élőben NEM futtattam (írás; t941 már igazolta) — a worker újraindítása után fut először.

### Aktiváláshoz
- **Worker ÉS API újraindítás kell** (user döntés). Sorrend: worker előbb vagy együtt — a régi worker a `sync_scout` jobot nem ismeri.

### Fájlok
`src/worker/{assets.py, scout_runner.py (új), queue.py, handlers/__init__.py, handlers/sync_full.py, handlers/sync_scout.py (új), tests/test_scout_runner.py (új)}`,
`src/api/{main.py, schemas.py, routers/jobs.py, routers/scout.py (új), services/scout.py (új), tests/test_scout_api.py (új)}`,
`src/cq_mcp/{server.py, tools_scout.py (új), tests/test_tools.py}`, `frontend/src/types/api.ts`, `frontend/src/api/scout.ts` (új);
doksi: `8300_api.md`, `8310_worker.md`, `8320_cq_mcp.md`, `5800_scout_runtime.md`.

**2026-11-23 (validator_agent) — VALIDÁLVA → done.**
- `uv run pytest src/api src/worker src/cq_mcp src/scout -q`: **134 passed**. ruff + pyright (16 érintett fájl): tiszta; `npx tsc --noEmit`: tiszta.
- Kód-review: Scout lépés csak Scout asseten (manifest train ∩ konfigurált, `scout.assets` override), új bár + `min_interval_s` throttle, lusta artifact-betöltés lockkal, `maybe_run_scout` sosem dob; `SCOUT ALERT` mismatch/missing esetén; önálló `sync_scout` job dedup `(kind, asset_id)`, BCH-t elutasítja tábla-létrehozás előtt. API read-only (`ro_connection`), 404 ismeretlen asset, BCH `available=false`, limit/recent `Query(ge, le)` → 422; meta manifest JSON-ból (nincs `scout` import). Jobs API 400/404 lefedve (`test_jobs_api_sync_scout`).
- **Valódi író út (worker újraindítás nélkül):** az ETH live DB másolatán (scratchpad) a `run_sync_scout` handler közvetlen hívása: 1. futás 8 esemény, 0 beszúrás (naprakész), 2. futás azonos → idempotens (sor-hash változatlan). A 2 legújabb sor törlése után: `inserted=2` (`backfilled=True`), `mismatch=0`, `missing=0`, a `event_id+p_impulse+status` hash bitre megegyezik az eredetivel. BCH → `NotScoutAssetError`; `maybe_run_scout`: `no_new_bars` / `throttled` / `not_scout_asset` ✔. A másolat törölve.
- Apró javítás: `8300_api.md` `POST /api/jobs` sor — dedup `(kind, asset_id)` + `sync_scout` 400/404 pontosítás.
- Nem blokkoló: (1) MCP `scout_signals` hibás `since` stringre nyers `ValueError` (nincs barátságos hibaüzenet); (2) `.agent/agents/ui_agent.md` „19 tool” → 20 (code_doc_agent); (3) aktiváláshoz worker + API újraindítás kell (user döntés).
