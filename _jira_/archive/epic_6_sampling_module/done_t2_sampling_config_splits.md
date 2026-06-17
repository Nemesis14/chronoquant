---
epic: epic_6
id: t2
title: Create sampling/config.py and sampling/splits.py
assignee: modeling_agent
status: pr
blocks: [t5]
---

## Goal
Létrehozni a `sampling/` csomag első két fájlját: a konfigurációs dataclass-t és a
tiszta date-math split generátort. Nincs DB-, IO-, vagy project import-függőség
— önállóan tesztelhetők.

## Scope
- `src/modeling/quantitative/sampling/` (mappa létrehozása)
- `src/modeling/quantitative/sampling/__init__.py` (üres placeholder, t5-ben töltjük ki)
- `src/modeling/quantitative/sampling/config.py` (új)
- `src/modeling/quantitative/sampling/splits.py` (új, refaktorálva a meglévő `sampling.py` `create_expanding_window_splits()`-ből)

## Acceptance Criteria
- [ ] `SamplingConfig` frozen dataclass minden mezővel (`sample_id`, `asset_id`, `target_col`, `target_horizon_minutes`, `min_train_days=730`, `valid_days=180`, `step_days=180`, `test_days=365`, `embargo_minutes: int | None = None`)
- [ ] `build_expanding_window_splits(data_start, data_end, min_train_days, valid_days, step_days, test_days, embargo_minutes) -> dict` — pure math, nincs IO
- [ ] Output fold kulcs: `"fold": 1` (1-indexed, meglévő `folds.json` formátum)
- [ ] Output struktúra: `{"folds": [...], "test": {"start": ..., "end": ...}}`
- [ ] Coding standard: modul docstring, Google-style docstrings, `# %%` markerek, alignment
- [ ] Pandas import **nincs** — csak stdlib (`datetime`) és ha kell `pandas.Timedelta` minimálisan
- [ ] `uv run pyright src/modeling/quantitative/sampling/config.py` hibátlan
- [ ] `uv run pyright src/modeling/quantitative/sampling/splits.py` hibátlan

## Notes
A meglévő `sampling.py::create_expanding_window_splits()` logikája újrahasználható,
de a docstring és stílus teljes refaktor. A `_format_time()` helper ide kerül vagy
`splits.py` privát függvénye lesz.
