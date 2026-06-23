---
id: t50
title: új strategy search engine implementáció percentile cutoff + TP/SL grid alapján
epic: epic_035_strategy_execution_search
assignee: modeling_agent
status: pr
blocks: [t51, t52]
blocked_by: [t49]
---

## Description

Lecseréli a jelenlegi `optimize.py` (Optuna TPE) alapú strategy optimizert egy determinisztikus grid search engine-re.

## Módosított fájlok

### 1. `src/strategy/strategy/calibrate.py`
- `_build_rank_lookup()`: hozzáadva `bucket_median_mfe` (np.median) és `bucket_p75_mfe` (np.percentile 75) per-bucket stats
- `fit_calibration()`: hozzáadva `bucket_median_mfe_long/short` és `bucket_p75_mfe_long/short` oszlopok a visszaadott DataFrame-ben; `_bucket_stat_map()` alkalmazva mindkét új stat-ra

### 2. `src/strategy/strategy/search.py` — ÚJ FÁJL
- `ENTRY_CUTOFFS`, `TP_SPECS`, `SL_SPECS` konstansok
- `_resolve_tp_lr()`, `_resolve_sl_lr()` helper függvények
- `_simulate_direction()`: TP/SL/timeout bar-by-bar szimuláció, SL nyeri konflikt esetén
- `_build_cutoffs()`: átemelve optimize.py-ból
- `search_strategy()`: grid search loop, best setup kiválasztása, strat.* táblák + artifact + registry írása

### 3. `src/strategy/strategy/optimize.py` — TÖRÖLVE
- Nem importálta semmi más → biztonságosan törölhető volt

### 4. `src/strategy/00_run_strategy_session.py` — módosítva
- Import csere: `search_strategy` from `strategy.strategy.search`
- `--n-trials` paraméter eltávolítva
- `--directions` paraméter hozzáadva (default: "long,short")
- `optimize_strategy` hívás → `search_strategy` hívás
- Output: top-5 setup nyomtatása is

### 5. `src/strategy/__init__.py` — módosítva
- Docstring frissítve: Optuna referenciák eltávolítva

## Notes

Implementálva, tesztelve. 23/23 pytest zöld. ruff + pyright 0 error az érintett fájlokon.

NULL-safe OHLCV kezelés: `high`, `low`, `close` None-check a `_simulate_direction`-ban; ha live.ohlcv unavailable → TP/SL nem tüzel, csak timeout exit lehetséges.

tp_lr <= 0 esetén skip (negatív bucket mean esetén ne lépjünk be pozícióba).
