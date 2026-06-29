---
epic: epic_043
id: t7
title: Code doc frissítés — binance_data, data.py, service.py, strategy.py
assignee: code_doc_agent
status: pr
blocks: []
blocked_by: []
---

## Goal

A `_doc_/database_and_code_doc/` zónában frissíteni a módosított fájlok dokumentációját.

## Scope

Érintett fájlok (kód megváltozott):
- `src/ui/binance_data.py` — `_normalize_futures` (csak closed fills), `current_position()`
- `src/ui/data.py` — `load_long_short_strategies()`, `_active_long/short_session_id()`, Binance fallback
- `src/trading/live/strategy.py` — `evaluate()` két cutoff paraméterrel
- `src/trading/live/service.py` — dual artifact load, dual rank lookup
- `src/ui/main.py` — `_render_strategy_card()` hívás eltávolítva
- `src/ui/components/trade_panel.py` — két strategy kártya, LONG/SHORT side color

Érintett doc fájlok:
- `_doc_/database_and_code_doc/` megfelelő aloldalak (UI, trading, data_handling)

## Acceptance Criteria

- [x] `binance_data.py` változások dokumentálva (szűrés logika, current_position)
- [x] `data.py` dual session load dokumentálva
- [x] `strategy.py` / `service.py` dual cutoff dokumentálva
- [x] UI layout változások code-szinten dokumentálva

## Notes

2026-06-28 — code_doc_agent végrehajtás

Frissített doc fájlok:

**`_doc_/database_and_code_doc/8150_ui_components.md`**
- `_normalize_futures`: dokumentálva a closed-fill szűrés (abs(realizedPnl) >= 0.001)
  és a BUY→SHORT / SELL→LONG side-mapping
- `current_position()`: új függvény dokumentálva (Binance futures pozíció lekérdezés,
  `_source == "binance"` jelzőmező)
- `_render_strategy_card(cfg, direction)`: új helper dokumentálva, zöld/piros
  irány-alapú fejléc-szín logika
- `_render_signal_trigger_card`: dokumentálva a két strategy kártya egymás melletti
  elrendezése (`st.columns(2)`)
- `_render_active_trade_card`: dokumentálva a LONG→zöld / SHORT→piros side_color logika

**`_doc_/database_and_code_doc/8120_ui_data.md`**
- Overview flowchart frissítve: dual artifact forrás + Binance API jelölve
- `load_long_short_strategies()`: dokumentálva a dual session betöltés
- `_active_long_session_id()` + `_active_short_session_id()`: új helperek dokumentálva
- `_load_session_artifact()`: dokumentálva
- `active_position()`: dokumentálva a Binance fallback ág
- `_load_rank_lookups()`: dokumentálva a session-specifikus long/short lookup betöltés

**`_doc_/database_and_code_doc/7150_trading_state_strategy.md`**
- `evaluate()` szignatúra frissítve: `entry_cutoff_long` és `entry_cutoff_short`
  opcionális paraméterek dokumentálva
- FLAT döntési szabályok frissítve: per-side cutoff logika, long prioritás dual jel esetén

**`_doc_/database_and_code_doc/7120_trading_service.md`**
- `__init__`: dual session artifact betöltés dokumentálva, dual rank lookup, per-session cutoff tárolás
- `_to_percentiles`: session-specifikus lookup használat dokumentálva
- `_cycle` sequence diagram frissítve: cutoff paraméterek átadása jelölve

**`_doc_/database_and_code_doc/8110_ui_main.md`**
- `_render_strategy_card()` hívás eltávolításának dokumentálása
