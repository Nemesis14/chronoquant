---
epic: epic_034
id: t46
title: Teljes ujrafuttatas az uj architekturaval
assignee: modeling_agent
status: pr
blocks: [t47]
blocked_by: [t42, t43, t44]
---

## Goal
A refaktor után minden releváns modeling és strategy lépést újra lefuttatni az új
architektúrán, és új artifactokat létrehozni.

## Scope
- sample
- feature_engineering
- search
- train
- predict
- strategy session

## Acceptance Criteria
- [x] Az érintett modellekhez új sample/FE/search/train/predict artifactok létrejönnek
- [x] A strategy session is újrafut az új modeling outputokon
- [x] A létrejött artifactok és registry/provenance bejegyzések konzisztensnek látszanak

## Notes
Ez az epic végső gyakorlati célja: ne csak át legyen írva a kód, hanem tényleg az új lánc fusson.

[2026-06-22] Teljes pipeline sikeresen lefutott — modeling_agent

**Modell:** `lgbm_solusdt_l_fw60_2101_2605` (az egyetlen aktív long modell; a taskban említett
`lgbm_solusdt_l_fw60_2021` nem létezik a models.json-ban — ez az ID nem volt implementálva)

**Pipeline lépések eredménye:**

1. **sample** — `model."lgbm_solusdt_l_fw60_2101_2605__sample"` létrejött: 47448 sor,
   5 fold (0=24072, 1=5856, 2=5880, 3=5808, 4=5832), feature_set=fs_solusdt_fw60_l__80ff5a16

2. **feature_engineering** — Notebook papermill-lel lefutott (25 cella), HTML render OK.
   `feature_set.json` provenance: snapshot_id=solusdt_fw60_2101_2605__21668185,
   sample_rows=47448, joined_rows=47448 (I1 invariáns: OK), source_contract="snap ⋈ model.__sample",
   selected=124 feature.
   registry: feature_set_id=fs_solusdt_fw60_l__80ff5a16

3. **search (smoke)** — 5 trial futott (stage=smoke), best trial #4: obj=-0.006431,
   mean_top10_lift=0.006518, mean_spearman=0.291365.
   Artifacts: `search/best_params.json`, `search/search_best.json` OK.

4. **train** — `model.pkl` (833201 bytes) létrejött: n_features=124, n_estimators=1794.

5. **predict** — `model."lgbm_solusdt_l_fw60_2101_2605__pred"` 2846880 sor,
   tartomány: 2021-01-01 - 2026-05-31 23:59:00. snapshot_immutable=True.

**Registry állapot:** `lgbm_solusdt_l_fw60_2101_2605` → status=predicted, snapshot_id OK,
feature_set_id=fs_solusdt_fw60_l__80ff5a16.

**Strategy session (opcionális — sikeresen futott):**
`strat_solusdt_fw60_combo_2101_2605` — calib: 2022-2023, opt: 2024-2025, n_trials=3.
best_objective=0.008045, n_trades=822, win_rate=0.8589, sharpe=27.42.
Artifacts: `strat.*` táblák + `strategy_artifact.json` + `reg.strategies` bejegyzés.

**Javított hibák:** Nem volt szükség javításra — minden lépés elsőre lefutott.
