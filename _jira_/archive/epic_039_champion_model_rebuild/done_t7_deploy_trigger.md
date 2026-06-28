---
epic: epic_039
id: t7
title: Deploy trigger — új artifact-ok aktiválása live trading-hez
assignee: database_agent
status: pr
blocks: []
blocked_by: [t6]
---

## Goal

Az új champion modellek és strategy artifact-ok aktiválása a live trading
pipeline-ban. A `sync_predictions` backfill + atomikus csere az új modellekre vált.

## Scope

- `uv run python src/data_handling/06_trigger_deploy.py --strategy-session-id <new_session_id>`

## Acceptance Criteria

- [ ] `reg.deployments` bejegyzés `pending` státuszban létrejött
- [ ] `sync_predictions` lefutott: backfill az új modell predikciókkal
- [ ] `predictions` tábla `long_model_id` és `short_model_id` az új champion modellekre mutat
- [ ] A deploy atomikus tranzakcióban történt (részleges csere nem fordulhat elő)
- [ ] `reg.deployments` bejegyzés `active` státuszra vált
- [ ] `config/trading.json` `strategy_session_id` konzisztens az új session-nel

## Notes

Deploy előtt ellenőrizni: az új `strategy_artifact.json` fájl betölthető-e
a trading service-ből. A live service újraindítás külön manuális lépés.

[database_agent] Deploy kész — 2026-06-23

**deployment_id:** `16b153ee-0d41-4e7b-800c-94d201df384c`

**Elfuttatott lépések:**
1. `strategy_artifact.json` validálva — JSON betölthető, összes artifact megvan
2. `reg.strategies` bejegyzés ellenőrizve: `strat_solusdt_fw60_combo_2101_2605` létezik, mindkét model_id helyes
3. Mindkét modell `predicted` státuszban: `lgbm_solusdt_l_fw60_2101_2605`, `lgbm_solusdt_s_fw60_2101_2605`
4. `06_trigger_deploy.py` lefutott — `reg.deployments` pending sor létrehozva
5. `sync_predictions` cutover lefutott atomikus tranzakcióban — `predictions` tábla cserélve

**Predictions tábla állapota cutover után:**
- Sorok száma: 2,846,880
- Időtartomány: 2021-01-01 → 2026-05-31
- `long_model_id`: `lgbm_solusdt_l_fw60_2101_2605`
- `short_model_id`: `lgbm_solusdt_s_fw60_2101_2605`

**reg.deployments állapot:** `active=True`, `status='active'`, `activated_at=2026-06-23 17:33:43`

**config/trading.json `strategy_session_id`:** `strat_solusdt_fw60_combo_2101_2605` — konzisztens
