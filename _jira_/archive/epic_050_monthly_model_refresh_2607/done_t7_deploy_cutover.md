---
epic: epic_050
id: t7
title: Éles csere — régi deployment kivezetése, új becseréslése
assignee: database_agent
status: done
blocks: [t8]
blocked_by: [t6]
---

## Goal

A jelenleg aktív deployment (`strat_solusdt_fw60_combo_2101_2605`) kivezetése
és az új champion (2101_2607) élesítése.

## Scope

`src/data_handling/06_trigger_deploy.py`, `reg.deployments`, `predictions` tábla cutover.

## Acceptance Criteria

- [x] **Explicit user-megerősítés a végrehajtás előtt** — magas blast-radius, éles kereskedést érintő lépés
- [x] `06_trigger_deploy.py --strategy-session-id <új_session_id>` lefut, `reg.deployments` pending bejegyzés létrejön
- [x] A soron következő `sync_predictions` ciklus végrehajtja a backfill+swap cutovert (atomikus tranzakció)
- [x] Régi deployment `active` → `archived`, új `pending` → `active`
- [x] `predictions` tábla `long_model_id`/`short_model_id` stampje az új modellekre mutat

## Notes

`config/models.json`: a két régi (2101_2605) entry törölve, a két új (2101_2607)
`active: true`-ra állítva. `06_trigger_deploy.py --strategy-session-id
strat_solusdt_fw60_long_2101_2607` → pending deployment (deployment_id
52214293-da95-41d8-b74f-3f3a76c3f907). `02_sync_pipeline.py --tables
predictions --skip-ohlcv` (266s, 24 chunk) végrehajtotta a cutovert.

Ellenőrizve: `reg.deployments` — régi (`strat_solusdt_fw60_combo_2101_2605`)
active=False/archived, új active=True/active, previous_strategy_id helyesen
láncolva. `predictions` tábla: mind a 2 951 152 sor egységesen
`long_model_id=lgbm_solusdt_l_fw60_2101_2607`,
`short_model_id=lgbm_solusdt_s_fw60_2101_2607` — nincs kevert/régi stamp.

User explicit jóváhagyásával a régi 2101_2605 artifact mappák (artifacts/
lgbm_solusdt_l_fw60_2101_2605/, .../s_fw60_2101_2605/, ~326MB) törölve —
git history-ban megvannak. Registry sorok (reg.models, reg.strategies, a
régi deployment archived bejegyzése) nem törölve — a dokumentált
életciklus szerint az "archived" a helyes végállapot, provenance-ként marad.
