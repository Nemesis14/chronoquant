---
epic: epic_006
id: t2
title: Modeling domain — fájlok mozgatása, family struktúra, script számozás
assignee: modeling_agent
status: todo
blocks: [t4]
---

## Goal
A jelenlegi `src/modeling/`, `src/evaluation/`, `src/elliott_waves/` modulokat átrendezni a `src/modeling/` alatti family struktúrába. Üres placeholder mappák a jövőbeli family-knek. Scripts számozva a `src/modeling/quantitative/` gyökerébe.

## Scope

**Mozgatások:**
- `src/modeling/*` → `src/modeling/quantitative/` (minden py fájl)
- `src/evaluation/*` → `src/modeling/quantitative/evaluation/`
- `src/elliott_waves/*` → `src/modeling/elliott/`

**Üres mappák létrehozása (jövőbeli family-k):**
- `src/modeling/text/`
- `src/modeling/blockchain/`
- `src/modeling/assembly/`

**Scripts (számozva, `src/modeling/quantitative/` gyökerébe):**
- `scripts/modeling/train_model.py` → `src/modeling/quantitative/01_train_model.py`
- `scripts/modeling/search_lgbm.py` → `src/modeling/quantitative/02_search_lgbm.py`
- `scripts/evaluation/backtest_strategy.py` → `src/modeling/quantitative/03_backtest_strategy.py`
- `scripts/evaluation/sweep_strategy.py` → `src/modeling/quantitative/04_sweep_strategy.py`
- `scripts/evaluation/generate_model_card.py` → `src/modeling/quantitative/05_generate_model_card.py`

**NE frissítsd az import path-okat** — az t4 feladata.

## Acceptance Criteria
- [ ] `src/modeling/quantitative/` tartalmazza a jelenlegi modeling/ fájlokat
- [ ] `src/modeling/quantitative/evaluation/` tartalmazza a jelenlegi evaluation/ fájlokat
- [ ] `src/modeling/elliott/` tartalmazza a jelenlegi elliott_waves/ fájlokat
- [ ] Üres mappák megvannak: `text/`, `blockchain/`, `assembly/`
- [ ] 5 script számozva megvan `src/modeling/quantitative/` alatt
- [ ] `src/modeling/` régi flat fájljai, `src/evaluation/`, `src/elliott_waves/` törölt
- [ ] `scripts/modeling/` és `scripts/evaluation/` törölt

## Notes
Import path-ok szándékosan érintetlenek — a kód t4-ig broken állapotban lesz.
Az `elliott_waves/` izolált marad, saját `__init__.py`-val.
