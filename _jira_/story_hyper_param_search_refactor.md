---
id: story_hyper_param_search_refactor
title: 02_hyper_param_search — spec + refaktor
assignee: modeling_agent
status: todo
---

## Goal

A `02_hyper_param_search.py` és a mögötte lévő `src/modeling/search/lgbm_search.py` refaktorálása az új sampling struktúra alapján. A régi kód expanding-window folds logikára épül — az új spec yearly random-hour sample + monthly valid weeks alapú CV-t használ.

## Scope

- `src/modeling/02_hyper_param_search.py`
- `src/modeling/search/lgbm_search.py`
- `src/modeling/search/__init__.py`

## Acceptance Criteria

- [ ] Input: `sample_train_valid.parquet` (yearly sample) + `feature_set.json`
- [ ] CV: monthly validation weeks alapján, purge sorok kizárva
- [ ] Output: `best_params.json` a megfelelő mappa alá
- [ ] CLI: `uv run python src/modeling/02_hyper_param_search.py --year 2021 --asset-id solusdt`
- [ ] `ruff check` + `pyright` tiszta
- [ ] Smoke teszt átmegy (vagy új smoke teszt kerül be)

## Notes

A régi `lgbm_search.py` expanding-window logikát használt. Ez teljesen ki van váltva a yearly sample alapú megközelítéssel.
Spec forrás: `_doc_/0000_project_overview.md` — "Yearly sample model" + "Script pipeline" szekciók.
