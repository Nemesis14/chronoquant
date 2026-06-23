---
id: t53
title: tesztek — search engine és artifact contract
epic: epic_035_strategy_execution_search
assignee: modeling_agent
status: pr
blocks: [t54]
blocked_by: [t51, t52]
---

## Description

Tesztek írása az új search engine-hez és az artifact contract frissítéséhez.

## Változások

### `src/strategy/tests/strategy/smoke/test_search.py` — ÚJ FÁJL (10 teszteset)
1. `test_long_tp_hit`: Long TP exit, fact_log_return ≈ tp_lr
2. `test_long_sl_hit`: Long SL exit, fact_log_return ≈ -sl_lr
3. `test_long_timeout`: Timeout (60 bar), exit_reason = "timeout"
4. `test_same_bar_tp_sl_conflict_sl_wins`: Ugyanazon bar TP+SL → SL nyeri
5. `test_reentry_after_exit`: Exit után következő bárban re-entry → 2 trade
6. `test_no_stop_loss_price_falls`: sl_spec="none" → nem SL
7. `test_short_tp_hit`: Short TP exit, fact_log_return ≈ tp_lr
8. `test_short_sl_hit`: Short SL exit, fact_log_return < 0
9. `test_no_trades_when_score_below_cutoff`: Nincs trade ha score < cutoff
10. `test_total_fact_log_return_equals_sum`: Összeg konzisztencia

### `src/strategy/tests/strategy/smoke/test_artifacts.py` — FRISSÍTVE
- `_DECISION_PARAMS`: Optuna shape → grid-search shape
- `_SEARCH_INFO`: új fixture
- `_METRICS`: új mezők (fact_log_return alapú)
- `_make_trade()`: frissítve új mezőkkel (fact_log_return, tp_lr, sl_lr, stb.)
- Régi assertionök (`optuna_best_trial`, `long_entry_pct`, `conflict_rule == "highest_edge"`) törölve
- Új assertionök: `decision_params["entry_cutoff"]`, `search_info["search_type"] == "grid"`, `summary_table` és `grid_results_table` kulcsok
- Hozzáadva: `test_write_realized_outputs_with_grid_results`, `test_register_strategy_writes_reg_rows` bővítve grid_results/summary artifact ellenőrzéssel

### `src/strategy/tests/strategy/smoke/test_optimize.py` — TÖRÖLVE
A régi Optuna-specifikus teszteket tartalmazta.

## Notes

Futtatás: `uv run pytest src/strategy/tests/ -v` → 23/23 PASSED (4.28s).
ruff: 0 error. pyright: 0 error.
