---
epic: epic_029
id: t131
title: Strategy API frissítés — dashboard strategy artifact olvasása
assignee: ui_agent
status: done
blocks: [t137]
blocked_by: [t130]
---

## Goal

A `data.load_long_short_strategies()` jelenleg `config/strategies.json`-ból olvassa az `entry_threshold`, `rearm_threshold`, `exit_threshold` mezőket — régi per-side strategy konfig struktúra, ami nem létezik az új rendszerben.

Az új strategy kontrakt: `strategy_artifact.json` → `decision_params` → `long_entry_pct`, `short_entry_pct`, `rearm_pct`, `min_edge_gap`.

A dashboard threshold display-t erre kell átállítani.

## Scope

- `src/ui/data.py` — `load_long_short_strategies()` rewrite
- `src/ui/components/trade_panel.py` (t130 után) — `_render_signal_trigger_card()` híváshelye
- `src/ui/main.py` — `render_asset_chart()` chart threshold paraméterei

## Acceptance Criteria

- [ ] `load_long_short_strategies()` beolvassa a `strategy_artifact.json`-t a `config/trading.json → strategy_session_id` alapján
- [ ] Return type: `(long_cfg: dict, short_cfg: dict)` ahol mindkét dict tartalmazza: `entry_pct`, `rearm_pct` mezőket
- [ ] `render_asset_chart()` chart-hívása `entry_threshold=long_cfg["entry_pct"]` stb. paraméterekkel dolgozik
- [ ] Ha nincs strategy artifact (session_id hiányzik), graceful fallback: üres dict
- [ ] `_render_signal_trigger_card()` a percentile-alapú entry-t mutatja (entry_pct, nem raw threshold)
- [ ] `uv run pyright src/ui/` tisztán fut
- [ ] `uv run ruff check src/ui/ --fix` tisztán fut

## Notes

A `strategy_artifact.json` path: `artifacts/{session_id}/strategy_artifact.json`.
A `session_id` forrása: `utils.load_trading_config()["strategy_session_id"]`.
Az artifact betöltéséhez már létezik: `from strategy.strategy.artifacts import read_strategy_artifact`.

A chart `prediction_price_figure()` hívása jelenleg `entry_threshold`, `rearm_threshold`, `exit_threshold` paramétereket vár (`charts.py`). Ezeket `long_entry_pct`, `rearm_pct` stb. értékekkel kell feltölteni — a chart paramétereinek neve változhat, ha a `charts.py` is frissítésre kerül, de az elfogadható ha az értékek logikailag helyesek.

A `strategies.json` fájl valószínűleg már nem létezik az új config struktúrában — ellenőrizni kell futtatás előtt.
