---
id: story_model_card_implement
title: 04_generate_model_card — spec + implementálás
assignee: modeling_agent
status: todo
blocked_by: [story_fit_model_implement]
---

## Goal

A `04_generate_model_card.py` refaktorálása. CV metrikák, OOS teljesítmény, feature importances összefoglalása.

## Scope

- `src/modeling/04_generate_model_card.py`
- `src/modeling/training/reports.py`

## Acceptance Criteria

- [ ] Input: `--model-id <id> --year 2021 --oos-year 2022`
- [ ] Beolvassa: `model.pkl`, `features.json`, `best_params.json`, `sample_oos.parquet`
- [ ] Output: `models/<model_id>/model_card.md`
- [ ] Tartalom: model ID, target, features száma, CV score, OOS AUC/Brier, feature importance top 20
- [ ] `ruff check` + `pyright` tiszta

## Notes

A model card dokumentum, nem artifact. A régi script stratégia-szintű backtestet is futtatott — ez a trading modul felelőssége.
