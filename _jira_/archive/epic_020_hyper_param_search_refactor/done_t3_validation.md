---
epic: epic_020
id: t3
title: Validáció — ruff + pyright + smoke teszt
assignee: validator_agent
status: todo
blocked_by: [t1]
---

## Goal

t1 pr_ státuszba kerülése után: statikus analízis + smoke search futtatása.

## Scope

- `src/modeling/search/lgbm_search.py`
- `src/modeling/02_hyper_param_search.py`
- `src/modeling/pipeline.py`

## Acceptance Criteria

- [ ] `ruff check src/modeling/ --fix` — tiszta
- [ ] `uv run pyright src/modeling/` — tiszta
- [ ] `uv run python src/modeling/02_hyper_param_search.py --model lgbm_solusdt_l_fw60_q90_2021 --stage smoke` — lefut, search_best.json + best_params.json keletkezik
- [ ] `uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_q90_2021 --step search --stage smoke` — lefut

## Notes

