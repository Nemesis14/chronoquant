---
epic: epic_068
id: t885
title: Meglévő riportok konvertálása az új formátumra
assignee: analyst_agent
status: done
blocks: [t890]
blocked_by: [t884]
---

## Goal
Az aktív modellek (champion `_2101_2605`) riportjai és a `_doc_/models_doc` általános elemzései újragenerálva az új formátumban. Régi .ipynb/.html nem konvertálódik: újragenerálás vagy rövid .md.

## Scope
`artifacts/<model_id>/reports/**`, `_doc_/models_doc/**`.

## Acceptance Criteria
- [x] Aktív modellek riportjai az Elemzések fülön
- [x] Nem újragenerálható régi elemzésből rövid .md konklúzió

## Notes

### Elkészült (analyst_agent)
- **Modell-riportok**: aktív modellek csak a két champion (`config/models.json`): `lgbm_solusdt_l_fw60_2101_2607` (t884 már megcsinálta, nem ismételve) és `lgbm_solusdt_s_fw60_2101_2607` (új: 01_sampling, 02_feature_engineering, 03_hyperparameter_search, 04_strategy). Az API 9 riportot listáz (4+4 modell + 1 általános).
- **Kimaradt**: az éves/2301 modellek (inactive a configban, artifactjuk törölve -> nincs input); a `_2101_2605` nem létezik.
- **Általános riport (új)**: `_doc_/reports/target_yearly_regime/` (builder: új `src/reports/builders/target_regime.py`, `build()`; a régi 3200 notebook újragenerálva). A többi régi notebook DB-/artifact-állapotra épült vagy elavult, nem generálható egyszerűen builderrel.
- **Md konklúziók** (`_doc_/models_doc/`): `target_yearly_regime.md`, `sampling_2101_2605.md`, `champion_search_2101_2605.md`, `model_2021_train_valid.md` (2 archive nb), `cluster_regime_oos_validation.md`, `signal_frequency_train_vs_live.md`; a meglévő 6500_v2/6501/6502 .md maradt. `README.md` index új.
- **Törölve** (`_doc_/models_doc/`): 5500, 6100, 6500, 7200 .ipynb+.html, 6500_README.txt, a teljes `archive/` (3200, 5600 x2 .ipynb+.html). `_files` mappa nem volt. Az `artifacts/<id>/` alatti régi html/ipynb érintetlen (t890).
- **Ellenőrzés**: uvicorn :8610 -> `/api/reports` 9 elem, general/target_yearly_regime és s/04_strategy 200; szerver leállítva. ruff + pyright (target_regime.py) + pytest src/reports lefuttatva.
- **Parancsok**: `uv run python src/modeling/pipeline.py --model lgbm_solusdt_s_fw60_2101_2607 --step analyze`; `cd src; uv run python -c "from reports.builders import target_regime as t; t.build()"`.
- Megjegyzés: a `_doc_/reports/target_yearly_regime/` a t884 szerint követett (nem gitignore-olt), nincs commitolva.

[validator] Done — 2026-11-06 (t892 epic-validáció)
Futtatva (egész-repo): ruff check src (All checks passed), pyright src (0 errors), pytest src -m 'not perf' (383 passed, 3 failed: élő-DB sanity — test_feat_ohlcv_quant_no_time_gap [14 időrés a live adatban], test_target_row_count_matches_features [target 3 120 401 vs feature 3 231 645, fw60 lemaradás + hiányzó sorok], test_predictions_score_range [nyers regressziós MFE score nem [0,1] — elavult feltételezés]; mind adatfüggő, kódhibára nem utal, az epic előtti ismert állapot), uv lock --check OK, frontend: tsc --noEmit + npm run build OK, registry_validator OK.
9 riport az API-n, olvasható (lásd t884).
