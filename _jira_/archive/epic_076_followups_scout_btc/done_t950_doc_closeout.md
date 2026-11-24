---
epic: epic_076
id: t950
title: Dokumentáció lezárás epic_074/075/076
assignee: code_doc_agent
status: done
blocks: []
blocked_by: [t947, t948, t949]
---

## Goal
Overview (`_doc_/0000_project_overview.md`): `src/scout/` modul (→ 5800), `artifacts/scout_s0c_r1/`, live `scout_signals` tábla, `/api/scout/*`, MCP 20 tool, worker `sync_scout` + funding, asset-sorrend, ETH data-only. `.agent/general_principles.md` „Aktív asset: SOLUSDT…” sor frissítése (SOL/BCH/BTC quant, ETH adat, Scout SOL/BTC/ETH). `.agent/agents/ui_agent.md` „19 tool” → 20. **CLAUDE.md Delegation Table: `src/scout/` (Impulse Scout runtime, artifact) → modeling_agent (user jóváhagyta 2026-11-24).** A t947–t949 doksi-változásainak átvezetése. Epic 074/075/076 epic.md Status → DONE.

## Acceptance Criteria
- [x] Overview, general_principles, ui_agent.md, CLAUDE.md frissítve
- [x] Epic státuszok lezárva (075, 076; a 074 archivált — nem nyúltam hozzá)

## Notes
- (orchestrátor) t949-ből: `_doc_/methodology_doc/1400_snapshots.md` `values_sha256` említés (methodology_agent zóna — t950 jelezze vagy rövid sorral vegye fel, ha a code_doc hatáskör engedi). Registry migráció v3 (`snapshots.values_sha256`) → overview registry rész.

### 2026-11-24 — code_doc_agent

**Változások:**
- `_doc_/0000_project_overview.md`: Impulse Scout bekezdés (Business Goal), új `src/scout/` modul-szekció (→ 5800); `src/api` → `/api/scout/signals` + `/latest`; worker job kindek + `sync_scout`, a `sync_full` lánca (OHLCV → funding → Scout → features → predictions, funding/Scout nem fatális); MCP 20 tool (18 olvasó, köztük `scout_signals`); Data Flow: `sync_funding`, `sync_scout`, `--repair-gaps`; Persistence: `content_sha256` + additív `values_sha256`, Scout artifact és forward-log sor; Repository Layout: `sync_tables` (funding/scout/`repair_gaps.py`), `src/scout/`, `tools_scout.py`, `artifacts/scout_s0c_r1/`, `database/btcusdt|ethusdt`; Database: LIVE `scout_signals`, REGISTRY migráció v3 (`snapshots.values_sha256`), asset-összegzés (SOL/BCH/BTC quant, ETH data-only, Scout SOL/BTC/ETH), worker lánc scout-tal, lyukjavítás bekezdés (`02_sync_pipeline.py --repair-gaps`); Live táblák: `funding_rate`, `scout_signals` (v8); Key Conventions: `sync_scout` író job; Agent Ownership: Modeling Agent + `src/scout/`. Az asset-sorrend (BTC, BCH, SOL, ETH), ETH data-only és BTC aktív modell már szerepelt (t946) — ellenőrizve.
- `.agent/general_principles.md`: „Aktív asset: SOLUSDT…” → aktív assetek sor (SOL/BCH/BTC quant, ETH data-only, Scout SOL/BTC/ETH kísérleti).
- `.agent/agents/ui_agent.md`: 19 → 20 tool (`scout_signals`); worker handlers + `sync_scout`, `scout_runner.py`; 5800 a doc-listában.
- `.agent/agents/modeling_agent.md`: 5800 a doc-listában; Scope sor `src/scout/`, `artifacts/scout_s0c_r1/`.
- `.agent/agents/database_agent.md`: 5800 a doc-listában (`sync_scout.py` / `scout_signals`).
- `CLAUDE.md` Delegation Table: `src/scout/` (Impulse Scout runtime + `artifacts/scout_s0c_r1/`) → `modeling_agent` (a `sync_scout.py` a `src/data_handling/` sorral database_agent marad).
- `epic_076/epic.md` Status → DONE (2026-11-24). `epic_075/epic.md` már DONE (2026-11-24) volt. epic_074 archivált → nem módosítva.
- A t947–t949 doksi-változásai (1200, 0003, 8310, 8350, 8320, 8300, 1410, 1510, 1310, 1100) a code_doc zónában már kész és validált; az overview-ba átvezetve.

**methodology_agent felé (nem módosítottam — kizárólagos zóna, kód-mentes, nem linkel le):** a `_doc_/methodology_doc/1400_snapshots.md` **nem létezik** (az 1400/1500 számot a `1400_features_volume_aktivitas.md` / `1500_features_kontextus.md` foglalja), és nincs `1500_registry.md` sem; ennek ellenére a `1410_snapshots_code.md` (13. és 316. sor) erre a kettőre linkel (a link relatív útvonala is hibás: `(1400_snapshots.md)`). Javasolt külön `todo_` ticket a methodology_agentnek: snapshot + registry módszertan doksi (immutability, a két hash szerepe: `content_sha256` = rács/sorszám/oszlopok, az id része; `values_sha256` = additív érték-hash változás-detektálásra), szabad számmal; utána a code_doc a 1410/1510 linkeket igazítja.


### 2026-11-24 — validator_agent

**Verdikt: PASS → done.** Doc-only ticket; ruff/pyright/pytest nem releváns (kódváltozás nincs).

**Ellenőrzés a kód ellen:**
- `src/scout/` (`runtime.py`: `load_artifact`, `run`; `freeze.py`, `tests/`) és `artifacts/scout_s0c_r1/` (`manifest.json`, `model.pkl`, `train_oof.parquet`) — létezik.
- `scout_signals`: `duckdb_store.py` migráció v8 (`_m008_create_scout_signals`); Scout assetek = `worker.assets.scout_asset_ids()` → artifact tanító assetjei (SOL/BTC/ETH), BCH kimarad — OK.
- API: `src/api/routers/scout.py` prefix `/api/scout`, `GET /signals` + `GET /latest` — OK.
- MCP: `src/cq_mcp/server.py` `TOOLS` = 20 (18 olvasó + `enqueue_sync`, `sentiment_refresh`; köztük `scout_signals`) — OK.
- Worker: `handlers/sync_full.py` sorrend OHLCV → funding (nem fatális) → Scout (`maybe_run_scout`, throttle 300 s) → features → predictions — OK; `queue.py` `JOB_KINDS` tartalmazza a `sync_scout`-ot — OK.
- Registry v3 `snapshots.values_sha256` (`registry.py` `_migration_003_snapshot_values_hash`) — OK; `02_sync_pipeline.py --repair-gaps [--dry-run]` → `repair_gaps.py` — OK.
- `config/assets.json` `display_order` = btc, bch, sol, eth; `trading.json` `ethusdt: {}` (data-only); `models.json` 11 modell, BTC pár `active: true` — OK.
- `CLAUDE.md` diff pontosan egy Delegation Table sor (`src/scout/` → `modeling_agent`) — OK.
- Relatív linkek: a módosított fájlokban nincs új törött link.

**Javítások (validator, apró):**
- `.agent/agents/database_agent.md` és `modeling_agent.md` „Aktív assetek” sor elavult volt (csak SOL/BCH) — kiegészítve BTC (élő) + ETH (data-only), összhangban a `general_principles.md`-vel.
- `_doc_/database_and_code_doc/8310_worker.md` `handlers/sync_full.py` sor összefoglalója: „OHLCV -> funding -> Scout -> features -> predictions”.

**Follow-up (nem blokkoló, meglévő hiba):** a `1410_snapshots_code.md` (13. és 316/318. sor) a nem létező `methodology_doc/1400_snapshots.md` és `1500_registry.md` oldalakra linkel (a link-target ráadásul `(1400_snapshots.md)` — rossz relatív útvonal); a HEAD-ben is így volt, nem a t950 vezette be. Javasolt külön `todo_` ticket methodology_agentnek (snapshot + registry módszertan), utána code_doc igazítja a linkeket.
