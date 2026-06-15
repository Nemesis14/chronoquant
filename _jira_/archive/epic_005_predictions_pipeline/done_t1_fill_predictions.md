---
epic: epic_005
id: t1
title: Predictions tábla feltöltése rebuild_derived.py --predictions-only
assignee: database_agent
status: done
blocks: [t2]
---

## Goal
A `predictions` tábla jelenleg üres. Fel kell tölteni a meglévő champion modellek
(lgbm_solusdt_l_fw60_q90_local_v4, lgbm_solusdt_s_fw60_q10_local_v4) segítségével
az összes elérhető OHLCV/feature időszakra.

## Scope
- Script: `scripts/data_pipeline/rebuild_derived.py --predictions-only`
- Érintett tábla: `predictions` (database/solusdt/solusdt.duckdb)
- Modellek: `models/lgbm_solusdt_l_fw60_q90_local_v4/`, `models/lgbm_solusdt_s_fw60_q10_local_v4/`

## Acceptance Criteria
- [ ] `predictions` tábla nem üres (row count > 0)
- [ ] long_pred és short_pred minden sorban [0, 1] tartományban van
- [ ] open_time lefedi a feat_ohlcv_quant időszakát (nincs nagy gap)
- [ ] Script hibamentesen lefut (exit code 0)

## Notes
A script chunk-onként fut (default 3 hónapos darabok), idempotens upsert.

Elvégzett munka:
- `config/models.json`: asset_id `solusdt_fw60` → `solusdt` (v4 modelleknél), champion model feloldás hiba javítva
- `src/data_pipeline/sync_predictions.py`: pd.NA → None konverzió target oszlopoknál (utolsó chunk edge case)
- `src/data_pipeline/rebuild_derived.py` → `scripts/data_pipeline/rebuild_derived.py` (áthelyezve, abszolút sys.path)
- Eredmény: 3,022,861 sor, 2020-09-14 → 2026-06-14, score-ok 100%-ban [0,1]-ben
