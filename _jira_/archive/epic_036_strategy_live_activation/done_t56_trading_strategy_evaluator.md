---
id: t56
epic: epic_036
title: Trading service strategy evaluator refactor
assignee: ui_agent
status: pr
blocks: [t57]
blocked_by: []
---

## Context

Az epic_035 után `strategy_artifact.json` → `decision_params` formátuma megváltozott:

**Régi (Optuna):** `long_entry_pct`, `short_entry_pct`, `rearm_pct`, `min_edge_gap`, `min_hold_minutes`, `cooldown_minutes`
**Új (grid search):** `entry_cutoff`, `tp_spec`, `sl_spec`, `directions`, `max_hold_minutes`, `same_bar_conflict_rule`

A trading service jelenleg crashel mivel a régi kulcsokat keresi.

**Aktív artifact:**
```
artifacts/strat_solusdt_fw60_combo_2101_2605/strategy_artifact.json
  decision_params.entry_cutoff = 0.97
  decision_params.max_hold_minutes = 60
  decision_params.directions = ["long", "short"]
```

**Invertált short ranking:** `short_mfe_fw60 = log(fw_min/close) < 0` → alacsonyabb
`score_pct_short` = jobb short. Entry feltétel shorthoz: `(1.0 - score_pct_short) >= entry_cutoff`.

## Notes

### Elvégzett változtatások

**`src/trading/live/strategy.py` — teljes rewrite:**
- `evaluate()` mostantól csak `entry_cutoff` és `max_hold_minutes` kulcsokat olvas a `decision_params`-ból
- FLAT state: long entry `score_pct_long >= entry_cutoff`; short entry `(1.0 - score_pct_short) >= entry_cutoff`
- Mindkét irány trigger esetén: long prioritás
- LONG/SHORT state: kizárólag `hold_minutes >= max_hold_minutes` → EXIT (nincs signal decay, nincs opposite edge exit)
- COOLDOWN branch teljesen eltávolítva
- `armed` check eltávolítva

**`src/trading/live/service.py`:**
- `_apply_cooldown_rearm()` metódus eltávolítva
- `_cycle()`-ből a hívása eltávolítva
- `_close_position()`: `cooldown_minutes` logika eltávolítva; `self.state.status = FLAT` (nem COOLDOWN)
- `_open_position()`: `self.state.armed = False` sor eltávolítva
- `from trading.live.state import COOLDOWN` eltávolítva

**`src/trading/live/state.py`:**
- `COOLDOWN = "COOLDOWN"` konstans eltávolítva
- `armed: bool = True` field eltávolítva
- `cooldown_until: datetime | None = None` field eltávolítva
- `from_db()`: `state.armed = False` sor eltávolítva
- Docstring frissítve: FLAT → LONG/SHORT → FLAT state machine

**`src/trading/tests/live/smoke/test_strategy.py` — teljes rewrite:**
- 8 régi teszt → 11 új teszt az új logikához
- Új `_DECISION_PARAMS` az új artifact formátumhoz (`entry_cutoff=0.97`)
- Short invertálás tesztelve: `score_pct_short=0.02` → `1-0.02=0.98 >= 0.97` → ENTER_SHORT
- Boundary tesztek: exact `entry_cutoff` értékre mindkét irányban
- Long prioritás tesztelve mindkét irány trigger esetén
- COOLDOWN tesztek eltávolítva; `armed` tesztek eltávolítva

**`src/trading/tests/live/smoke/test_state.py`:**
- `armed` field tesztek eltávolítva
- `test_from_db_no_position_returns_flat_armed` → `test_from_db_no_position_returns_flat`

### Validation eredmények

- `ruff check src/trading/ --fix` — All checks passed
- `uv run pyright src/trading/` — 0 errors, 0 warnings, 0 informations
- `uv run pytest src/trading/tests/ -v` — 16 passed in 0.31s
