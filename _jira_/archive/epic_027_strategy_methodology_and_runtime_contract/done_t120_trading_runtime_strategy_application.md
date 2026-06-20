---
epic: epic_027
id: t120
title: Alkalmazd a strategy artifactot a trading runtime-ban
assignee: ui_agent
status: done
blocks: [t121]
blocked_by: [t117, t119]
---

## Goal

A `src/trading/` runtime ugyanazt a strategy-transzformációt és döntési logikát használja,
amit az offline strategy artifact definiál.

## Scope

- `src/trading/live/strategy.py`
- `src/trading/live/service.py`
- strategy artifact betöltés
- runtime lookup alkalmazás

## Acceptance Criteria

- [x] A trading runtime a strategy artifactból tölti a szükséges lookupokat és paramétereket
- [x] A live döntés nem raw score-thresholdokra, hanem a strategy contractra épül
- [x] A conflict rule, cooldown, rearm és exit logika egyezik az offline contracttal
- [x] A journalból visszaellenőrizhető, hogy mely strategy mezők alapján született döntés

## Notes

Ez a task a tradinget mint alkalmazó réteget kezeli, nem strategy-képző modulként.

Implementálva: strategy.py és service.py átírva rank-first logikával.
- evaluate() szignatúra: score_pct_long/short + decision_params (pred_long/short és long/short_cfg törölve)
- service.py: read_strategy_artifact() betöltés, _to_percentiles() helper np.interp-pel, _apply_cooldown_rearm rearm_pct alapján
- config/trading.json: long/short_strategy_id -> strategy_session_id, asset_id: solusdt
- 9 smoke test: ruff + pyright 0 hiba, pytest 9/9 passed

### Validator (t121) — 2026-06-20

- ruff check src/trading/live/strategy.py + service.py: 0 errors (all checks passed)
- pyright src/trading/live/strategy.py: 0 errors, 0 warnings
- pyright src/trading/live/service.py: 0 errors, 0 warnings
- pytest src/trading/tests/: 14/14 passed
- status: done
