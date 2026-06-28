---
epic: epic_039
id: t5
title: Pipeline újrafuttatás — train + predict (long + short)
assignee: modeling_agent
status: pr
blocks: [t6]
blocked_by: [t4]
---

## Goal

A két champion modell train és predict lépéseinek újrafuttatása a t4-ben keletkezett
új best params alapján. Az eredmény: új `model.pkl` + `model.__pred` táblák.

## Scope

- `uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2101_2605 --step train`
- `uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2101_2605 --step predict`
- `uv run python src/modeling/pipeline.py --model lgbm_solusdt_s_fw60_2101_2605 --step train`
- `uv run python src/modeling/pipeline.py --model lgbm_solusdt_s_fw60_2101_2605 --step predict`

## Acceptance Criteria

- [x] `artifacts/lgbm_solusdt_l_fw60_2101_2605/model.pkl` frissítve (4,454 KB)
- [x] `artifacts/lgbm_solusdt_s_fw60_2101_2605/model.pkl` frissítve (1,761 KB)
- [x] `model."lgbm_solusdt_l_fw60_2101_2605__pred"` létezik (2,846,880 sor)
- [x] `model."lgbm_solusdt_s_fw60_2101_2605__pred"` létezik (2,846,880 sor)
- [x] `reg.models` bejegyzések `predict_done` státuszban
- [x] `manifest.json` mindkét modellhez frissítve

## Notes

**Long modell:** n_features=124, n_estimators=3296, pred rows=2,846,880
**Short modell:** n_features=78, n_estimators=902, pred rows=2,846,880

**predict.py bugfix (orchestrátor):** MemoryError a teljes snapshot pandas-ba töltésekor.
Megoldás: chunked predict implementálva (`_score_snapshot_chunked`, 250K sor/batch).
A sklearn feature name warning a numpy konverzió miatt jelenik meg — eredmények helyesek
(feature ordering garantált a `selected_features` lista alapján).

**Streamlit UI:** ideiglenesen kétszer leállítva a DuckDB file lock feloldásához
(short search és short predict futtatásához). Kézzel újraindítandó az orchestráció végén.
