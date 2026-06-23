---
id: t51
title: report contract frissítése artifacts.py-ban
epic: epic_035_strategy_execution_search
assignee: modeling_agent
status: pr
blocks: [t53]
blocked_by: [t50]
---

## Description

`artifacts.py` frissítése az új grid-search trade dict shape-hez és az új táblák hozzáadásához.

## Változások

### `_trades_dataframe()`
Új oszlopok a trade dict-ből: `fact_log_return`, `tp_lr`, `sl_lr`, `entry_cutoff`, `tp_spec`, `sl_spec`.
`bucket_mean_mfe` megmarad (visszafelé kompatibilitáshoz).

### `_equity_dataframe()`
Új oszlopok: `fact_log_return`, `cumulative_fact_log_return`.
`cumulative_mfe` megmarad (backward compat).

### `_summary_dataframe()` — ÚJ FÜGGVÉNY
1-row aggregált summary: n_trades, avg_entry/exit_price, avg_profit_pct, avg_expected_log_return, avg_fact_log_return, total_fact_log_return, compounded_return_pct, realized_directional_win_rate, avg_hold_minutes, take_profit_spec, stop_loss_spec.

### `_grid_results_dataframe()` — ÚJ FÜGGVÉNY
Grid search összes setup-ja: direction, entry_cutoff, tp_spec, sl_spec, n_trades, total_fact_log_return, avg_fact_log_return, compounded_return_pct, win_rate, avg_hold_minutes.

### `write_realized_outputs()` bővítése
Új paraméterek: `grid_results`, `best_setup`.
Új táblák: `strat."<session>__summary"` és `strat."<session>__grid_results"`.
Return dict bővítve erre a két táblára.

## Notes

Implementálva. 23/23 pytest zöld. ruff + pyright 0 error.
