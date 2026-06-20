---
epic: epic_029
id: t133
title: Stale backtest paths cleanup — artifacts/{session_id}/
assignee: ui_agent
status: done
blocks: [t137]
blocked_by: []
---

## Goal

A `data.py` három függvénye a régi `backtests/solusdt_long_fw60_q90_local_v3/` mappára hivatkozik fallback-ként:
- `backtest_summary()` → `summary.json`
- `closed_trades()` → `trades.csv`
- `equity_curve()` → `equity_curve.csv`

Ez a mappa nem létezik. Az új rendszerben ezek az adatok `artifacts/{session_id}/` alatt vannak:
- `summary.json`
- `trades.parquet`
- `equity_curve.parquet`

## Scope

- `src/ui/data.py` — `backtest_summary()`, `closed_trades()`, `equity_curve()`

## Acceptance Criteria

- [ ] A stale `backtests/solusdt_long_fw60_q90_local_v3/` path referenciák eltávolítva
- [ ] Fallback logika: ha nincs nyitott `trading_positions` tábla a `trading.db`-ben, olvassa a strategy artifact session könyvtárából a megfelelő parquet/json fájlt
- [ ] `session_id` forrása: `utils.load_trading_config()["strategy_session_id"]`
- [ ] Ha a session artifact sem létezik, graceful return: üres DataFrame / üres dict
- [ ] `closed_trades()` → `trades.parquet` (pandas) olvasás
- [ ] `equity_curve()` → `equity_curve.parquet` (pandas) olvasás
- [ ] `backtest_summary()` → `summary.json` olvasás
- [ ] `uv run pyright src/ui/data.py` tisztán fut

## Notes

A `trades.parquet` és `equity_curve.parquet` a strategy artifact session könyvtárában élnek: `artifacts/{session_id}/`. Ez a jelenlegi strategy pipeline outputja.

Ha a live `trading.db` már tartalmaz closed trade-eket, azok elsőbbséget kapnak a strategy artifact adataival szemben (maradjon meg a `closed_trades()` jelenlegi DuckDB-first logikája, csak a CSV fallback cserélődik parquet-ra).
