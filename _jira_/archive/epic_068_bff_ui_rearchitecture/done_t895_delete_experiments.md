---
epic: epic_068
id: t895
title: modeling/experiments + analyst_signal törlése, statsmodels kivezetése
assignee: modeling_agent
status: done
blocks: [t894, t890]
blocked_by: [t886]
---

## Goal
Kutatási kód csak md-ben marad. A t888 kérdései a user döntése (A) alapján: minden törlendő.

## Scope
`src/modeling/experiments/**` (25 fájl, köztük `cross_sectional_probe`), `src/modeling/analyst_signal/**`, tesztjeik. A `_doc_/research/*.md` fájlokban az experiments-re mutató linkeket cseréld szöveges hivatkozásra (a tartalom marad). `statsmodels` függőség kivétele a pyproject-ből, ha utána nincs használó (rg). A `lgbm_search` `gap_penalty` paraméter: maradhat, ha a `search_joint_reg_gp*` reprodukálás nem kell, akkor töröld és a Notes-ban jelezd. A t886 md-összefoglalói ELŐBB elkészülnek, mielőtt a kód törlődik: ellenőrizd, hogy minden törlendő kísérlethez van md.

## Acceptance Criteria
- [x] Minden törölt kísérlethez van md a `_doc_/research/` alatt
- [x] `src/modeling/experiments` és `analyst_signal` nem létezik
- [x] Nincs törött link a `_doc_/research`-ben
- [x] pytest, ruff, pyright zöld (a pyright hibák az experiments-szel együtt elmennek)

## Notes

[modeling_agent] Elvégzve.
- Törölve: `src/modeling/experiments/**` (24 .py + `__init__`, köztük `cross_sectional_probe`, `cross_sectional_ranker`), `src/modeling/analyst_signal/**` (gate_measurement, score_authors, social_features, validate), tesztek: `src/modeling/tests/analyst_signal/**`. Kívülről 0 import (rg: src/api, worker, cq_mcp, reports, pipeline, scripts, tesztek); a `data_handling` csak az `analyst_signal_store`-t használja (marad, t894/t887).
- md-lefedettség: mind a 24+4 fájl neve szerepel `_doc_/research/*.md`-ben (README index 19 sor), "Kód a git historyban" lábléc (commit `73f2156`); új md nem kellett.
- Linkek: `sol_8h_lgbm_gate.md` (3), `champion_streak_gate.md` (1) fájl-linkje szöveges `kód` hivatkozásra cserélve; más törött experiments-link nincs. `joint_feature_param_search.md`: gap_penalty megjegyzés frissítve.
- statsmodels: `pyproject.toml`-ból kivéve (rg: nincs használó), `uv lock` -> `Removed patsy, statsmodels`; `uv lock --check` OK. A `uv sync` valódi futása fájlzárolás miatt hibázott (`google/_upb/_message.pyd` Access denied, futó folyamat), `uv sync --dry-run` OK. A lock diffje a pyproject más (nem t895-ös: streamlit ki, fastapi/uvicorn/mcp be) párhuzamos módosításait is tartalmazza. `sync_predictions.py`: a `statsmodels` trainer-ág (const oszlop, early return) és a `trainer` paraméter eltávolítva.
- gap_penalty: törölve a `lgbm_search.run_search`, `_search_optuna`, `_compute_objective` paraméterből és a `joint_reg_gp*` tag-ból (joint search dir mindig `search_joint`); a `penalized_ratio` kimeneti kulcs marad (= valid_ratio). A régi gp-kísérletek nem reprodukálhatók (nem kell). `_doc_/methodology_doc/5000_hyper_param_search.md` gap_penalty szakasza elavult (methodology_agent/t891).
- Parancsok: `uv run ruff check src/modeling ... --fix` -> clean; `uv run pyright src/modeling src/strategy` -> 0 hiba (volt 246); `uv run pytest src/modeling src/strategy src/reports src/data_handling --deselect perf` -> 330 passed, 3 failed (élő-DB sanity: test_features_target x2, test_predictions_score_range; nem kapcsolódik).

[validator] Done — 2026-11-06 (t892 epic-validáció)
Futtatva (egész-repo): ruff check src (All checks passed), pyright src (0 errors), pytest src -m 'not perf' (383 passed, 3 failed: élő-DB sanity — test_feat_ohlcv_quant_no_time_gap [14 időrés a live adatban], test_target_row_count_matches_features [target 3 120 401 vs feature 3 231 645, fw60 lemaradás + hiányzó sorok], test_predictions_score_range [nyers regressziós MFE score nem [0,1] — elavult feltételezés]; mind adatfüggő, kódhibára nem utal, az epic előtti ismert állapot), uv lock --check OK, frontend: tsc --noEmit + npm run build OK, registry_validator OK.
