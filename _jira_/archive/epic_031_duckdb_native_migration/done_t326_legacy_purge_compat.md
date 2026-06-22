---
epic: epic_031
id: t326
title: Régi objektumok/refek eldobása + trading/UI kompatibilitás zárás
assignee: database_agent
status: done
blocks: []
blocked_by: [t325]
---

## Goal
A régi (átállás előtti) DuckDB objektumok, model-hivatkozások és JSON-maradványok
végleges eltakarítása, hogy a változtatás végén kizárólag az új, registry-vezérelt
struktúra éljen. Plusz a trading és UI tényleges kompatibilitásának záró ellenőrzése
az új táblákkal és naminggel.

## Scope
- Legacy DuckDB objektumok eldobása: a 10 éves modellhez tartozó model.*__sample / __pred
  táblák, elárvult snap.* / strat.* objektumok (amik nem a 2 championhez tartoznak)
- `config/` purge: minden nem létező / elavult model_id hivatkozás kitakarítása
  (strategies.json a 2 champion strategy session-re mutasson; models.json már a t325-ben szűkült)
- backtests/ és egyéb elárvult artifact-referenciák felszámolása
- trading: `config/trading.json` strategy_session_id az új session-re; live betöltés zöld
- UI smoke: trades/equity a strat.* táblákból; dashboard hibamentesen indul
- registry-validátor lefuttatása (t318): config ↔ reg.* konzisztens

## Acceptance Criteria
- [x] nincs élő hivatkozás a régi (éves / *_local_v4 / q90 / q10) model_id-kra sehol a configban
- [x] elárvult DuckDB objektumok eldobva; csak a 2 champion + élő táblák maradnak
- [x] trading service betölti az új strategy_artifact-ot; smoke OK
- [x] UI smoke OK az új strat.* táblákkal
- [x] registry-validátor zöld (config ↔ registry)
- [x] eltakarított elemek listája a Notes szekcióban

## Notes

### Elvégzett munka (2026-06-22)

**DuckDB objektumok állapota — lab.duckdb:**
- `snap` séma: csak `solusdt_fw60_2101_2605__21668185` — OK, nincs elárvult snapshot
- `model` séma: csak `lgbm_solusdt_l/s_fw60_2101_2605__sample` + `__pred` — OK, nincs éves modell
- `strat` séma: csak `strat_solusdt_fw60_combo_2101_2605__trades/__equity/__cutoffs` — OK

Nem volt szükség DROP TABLE parancsra — a lab.duckdb már t325-ben tisztán lett létrehozva.

**config/trading.json — módosítva:**
- `strategy_session_id`: `strategy_lgbm_solusdt_fw60_2101_2605` → `strat_solusdt_fw60_combo_2101_2605`

**config/strategies.json — teljes csere (schema_version 2 → 3):**
- Eltávolítva: 8 legacy backtest entry (`solusdt_long/short_fw60_q90/q10_local_v1/v2/v3/v4`, `_managed_v1`)
- Hozzáadva: egyetlen champion session entry: `strat_solusdt_fw60_combo_2101_2605`
  - `model_id_long`: `lgbm_solusdt_l_fw60_2101_2605`
  - `model_id_short`: `lgbm_solusdt_s_fw60_2101_2605`
  - `signal_mode`: `rank_first`

**config/models.json — nem módosítva:**
- Már t325-ben szűkítve; csak a 2 champion szerepel — OK

**src/data_handling/store/registry_validator.py — bugfix:**
- A validator `SELECT model_id, asset_id FROM reg.models` lekérdezést futtatott,
  de a `models` táblában nincs `asset_id` oszlop → az exception elnyelődött → minden modell
  "missing"-ként jelent meg. Javítva: `SELECT model_id FROM reg.models` (set-alapú lookup),
  `asset_id_mismatches` check eltávolítva (az oszlop nem létezik a sémában).

### Smoke check eredmények

**Registry validátor:** `OK — config and registry are consistent`

**Trading smoke:**
- `strategy_session_id`: `strat_solusdt_fw60_combo_2101_2605` ✓
- `strategy_artifact.json`: OK (signal_mode=rank_first, 134 trades)
- `rank_lookup_long/short.parquet`: OK

**UI smoke (strat.* táblák):**
- `strat.strat_solusdt_fw60_combo_2101_2605__cutoffs`: 20 sor
- `strat.strat_solusdt_fw60_combo_2101_2605__equity`: 134 sor
- `strat.strat_solusdt_fw60_combo_2101_2605__trades`: 134 sor

**DuckDB stats validátor (`01_validate_stats.py`):** minden tábla OK
- ohlcv: 3,033,002 sor | predictions: 3,033,002 sor | feat_ohlcv_quant: 3,033,002 sor

**Ruff + Pyright (registry_validator.py):** 0 hiba
