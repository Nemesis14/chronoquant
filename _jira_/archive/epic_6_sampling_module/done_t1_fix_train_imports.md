---
epic: epic_6
id: t1
title: Fix broken imports in train.py
assignee: modeling_agent
status: pr
blocks: [t6]
---

## Goal
`src/modeling/quantitative/train.py` importál két törölt fájlt: `lasso_logreg` és
`statsmodels_logreg`. Amíg ez megvan, `01_train_model.py` nem fut. Ki kell szedni a
dead import-okat és a hozzájuk tartozó dispatcher ágakat.

## Scope
- `src/modeling/quantitative/train.py`

## Acceptance Criteria
- [ ] `from modeling.quantitative.lasso_logreg import ...` sor törölve
- [ ] `from modeling.quantitative.statsmodels_logreg import ...` sor törölve
- [ ] `if trainer == "sklearn_lasso_logreg":` blokk törölve
- [ ] `if trainer == "statsmodels_pvalue_logreg":` blokk törölve
- [ ] `train.py` coding standard szerint: modul docstring, `# %%` markerek, Google-style docstring a `train_model()`-en
- [ ] `uv run pyright src/modeling/quantitative/train.py` hibátlan

## Notes
`train.py` a session előtt már törölt volt (staged deletion). Új, tiszta `train.py` létrehozva:
`train_model(model_id)` dispatche-el csak `lightgbm_binary`-ra. Dead imports és dispatcher ágak
nem kerültek be. Pyright-ellenőrzés validator taskra halasztva (t7).
