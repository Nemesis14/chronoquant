---
epic: epic_068
id: t888
title: Dead code + redundancia audit és refaktor: modeling + strategy
assignee: modeling_agent
status: done
blocks: [t890]
blocked_by: [t880, t881]
---

## Goal
Mint t887; kiemelten: legacy sampling módok, `_experimental` útvonalak, `text/` placeholder, notebook-függés (`01_feature_engineering.ipynb`), papermill/nbformat használat.

## Scope
`src/modeling/**`, `src/strategy/**`

## Acceptance Criteria
- [x] Dead code lista a Notes-ban
- [x] Törlések megtörténtek
- [x] pytest, ruff, pyright zöld (pyright: a `src/modeling/experiments/**` 241 régi hibája változatlan, lásd kérdések)

## Notes

### Dead code lista (bizonyíték: rg + AST hivatkozás-leltár `src/**`, `.agent`, `config`, pyproject; mind a 7 `config/models.json` modell `sampling_mode=train_valid_split`)
Törölt fájlok:
- `src/modeling/01_feature_engineering.ipynb|.html` - a pipeline FE lépése a `reports.builders.feature_engineering`-et hívja (t884); `rg`: csak doc-hivatkozás.
- `src/modeling/search/search_report.ipynb` - 0 hívó (csak régi artifact HTML-ek).
- `sampling/yearly_sampler.py` (`select_hourly_observations`, `assign_fold_ids`, `generate_walk_forward_folds`, `assign_walk_forward_fold_ids`), `sampling/config.py` (`Yearly/WalkForward/TrainValidSplitConfig`), `sampling/artifacts.py` (régi expanding-window artifact IO), `sampling/audit.py` (quant_train audit) - nincs prod hívó, csak teszt/`__init__`. A walk-forward minta (`fold_id`) downstreamben (search, fit, reports) már nem fogyasztható: mindenhol `split` van.
- `training/cv.py` (PurgedEmbargoCV), `training/datasets.py`, `training/metrics.py`, `training/reports.py` (matplotlib), `training/training_windows.py` - 0 külső hívó (régi pre-DuckDB training; `DatasetSplit` az egyetlen élő elem -> `search/lgbm_search.py`-ba költöztetve).
- Tesztek: `test_artifacts`, `test_audit`, `test_walk_forward_config`, `test_hourly_selection`, `test_segment_assignment`, `test_monthly_validation_weeks`, `test_walk_forward_folds`, `tests/smoke/test_pipeline_fe_step.py` (a t884 előtti papermill-t mockolta, 4 teszt már a baseline-on is bukott), valamint a `_check_patience` és a tautologikus `n_estimators` tesztek.
Törölt függvények/ágak:
- `create_sample.create_snapshot_sample` + walk-forward ág, `_anchor_year`, `_fold_counts`; `snapshot_sampler.build_fold_case_sql/build_sample_select_sql/build_sample_ctas_sql`; `sampling/__init__` csak a `train_valid_split` API-t exportálja. `sampling_mode` hiányzó kulcs alapértéke `train_valid_split` (volt: `walk_forward`); más érték ValueError.
- `lgbm_search`: `_search_random`, `_sample_params_random`, `_build_tpe_guide`, `_HAS_OPTUNA` (optuna kötelező dependency -> a fallback elérhetetlen), `_check_patience` (soha nem hívott).
- `training/artifacts.save_training_artifacts` + `_write_json` (0 hívó; `register_training_artifacts` marad), `fit_lgbm` legacy `fold_summary` ág (egyetlen artifact sem használja).
- `predict._read_snapshot_features/_score/_write_pred_table` (a chunked út váltotta), duplikált `_PREDICT_CHUNK_SIZE`; `strategy/search._resolve_tp_lr/_resolve_sl_lr` (a vektoros `_tp_lr_array/_sl_lr_array` váltotta).
- `pipeline.py`: `--fold-limit` (soha nem használt), `_BUILDER_MODULES` beolvasztva `_ANALYSIS_REPORTS`-ba; `--help` cp1250 `UnicodeEncodeError` javítva (`->`).
Nem létező: `text/` placeholder, `evaluation/`, `yearly_random_hour` a kódban/configban (csak `_doc_/0000_project_overview.md`-ben szerepel).
Bugfix: `00_create_sample.py` a `fold_row_counts`-ra hivatkozott (train_valid_split-nél KeyError) -> `split_row_counts`.
Tesztek: új, gyors (`generate_series`) end-to-end teszt a train/valid sample útra; a régi 2x88 s-os walk-forward e2e helyett.
Pyright pótlás: `tests/analyst_signal` 5 régi típushiba javítva (NotRequired TypedDict, float cast).

### Nem eldönthető jelöltek (kérdések)
1. `src/modeling/experiments/` (25 fájl): 0 import kívülről, nincs CLI/doc-parancs; DE `_doc_/research/{sol_8h_lgbm_gate,champion_streak_gate}.md` 4 fájlra linkel (sol_lgbm_8h, _econ, _audit, null_distribution, champion_streak_test), és 241 pyright hiba van bennük. Törölhető-e (a doksi-linkelteket meghagyva / mind)? Ettől függ a `statsmodels` (csak 6 experiments fájl + 1 string említés a `sync_predictions.py`-ban -> kivehető ha törlődnek).
2. `matplotlib`: a `training/reports.py` törlése után már csak `analyst/lib/plot_utils.py` + `research/ma_cross` használja -> t890 `analyst/` sorsától függ. `papermill`: `src/`-ben már 0 (csak `analyst/doc_renderer/build_doc_notebook.py`) -> t890 `analyst/`-tal együtt.
3. `src/modeling/analyst_signal/` (altdata; tesztek zöldek, `analyst_signal_store` t887) - érintetlen, külön döntés.
4. `lgbm_search` `gap_penalty` (CLI/pipeline nem adja át, csak programból; a `search_joint_reg_gp*` régi kísérleteket reprodukálja) - meghagytam.
5. Doksi-hivatkozások a törölt kódra (t891): `_doc_/database_and_code_doc/{5050,5100,5200,5300,5410,5420,5510,5520,5530,0004}`, `_doc_/methodology_doc/{3000,5000}`, `_doc_/0000_project_overview.md` (yearly_random_hour, walk-forward).

### Parancsok / eredmények
- Baseline: `uv run pytest src/modeling src/strategy src/reports` -> 200 passed, 4 failed (test_pipeline_fe_step, stale); pyright 246 hiba (241 experiments + 5 analyst_signal tests); ruff: ipynb hibák.
- Után: pytest 130 passed (112 s, volt 298 s) + analyst_signal 20 passed; `ruff check src/modeling src/strategy --fix` -> All checks passed; pyright 241 hiba, mind `experiments/` (régi, változatlan); `--help` smoke: pipeline, 00_create_sample, 02, 03, 00_run_strategy_session OK; import smoke OK. Live DB-t érintő lépés (setup/sample) nem futott (regisztrációt ír), helyette in-memory e2e teszt.
- Registry/DB/artifacts adat nem törölt. Érintetlen: src/api, worker, cq_mcp, data_handling, ui, trading, frontend, reports.

[validator] Done — 2026-11-06 (t892 epic-validáció)
Futtatva (egész-repo): ruff check src (All checks passed), pyright src (0 errors), pytest src -m 'not perf' (383 passed, 3 failed: élő-DB sanity — test_feat_ohlcv_quant_no_time_gap [14 időrés a live adatban], test_target_row_count_matches_features [target 3 120 401 vs feature 3 231 645, fw60 lemaradás + hiányzó sorok], test_predictions_score_range [nyers regressziós MFE score nem [0,1] — elavult feltételezés]; mind adatfüggő, kódhibára nem utal, az epic előtti ismert állapot), uv lock --check OK, frontend: tsc --noEmit + npm run build OK, registry_validator OK.
