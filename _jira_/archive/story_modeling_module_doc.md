---
id: story_modeling_module_doc
title: Modeling modul _doc_ oldalak
assignee: code_doc_agent
status: todo
blocked_by: [story_fit_model_implement, story_model_card_implement]
---

## Goal

A `src/modeling/` modul teljes dokumentálása a `_doc_/modeling/` mappában.

## Scope

- `_doc_/modeling/overview.md` — modul célja, script pipeline, alkönyvtárak
- `_doc_/modeling/sampling.md` — yearly sample logika, szegmensek, fájlok
- `_doc_/modeling/feature_engineering.md` — már létezik (epic_018 t2)
- `_doc_/modeling/training.md` — LightGBM trainer, CV, datasets, metrics
- `_doc_/modeling/evaluation.md` — backtest, metrics

## Acceptance Criteria

- [ ] `_doc_/modeling/overview.md` létezik, konzisztens az `_doc_/0000_project_overview.md`-vel
- [ ] `_doc_/modeling/sampling.md` lefedi a spec-et
- [ ] `_doc_/modeling/training.md` leírja a fit flow-t
- [ ] Minden doc fájl konzisztens egymással

## Notes

`_doc_/modeling/feature_engineering.md` már elkészült. Elsősorban az agenteknek szól — tömör, pontos.
