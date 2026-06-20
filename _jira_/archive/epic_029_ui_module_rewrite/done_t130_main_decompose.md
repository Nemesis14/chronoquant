---
epic: epic_029
id: t130
title: main.py szétbontás komponensekre
assignee: ui_agent
status: done
blocks: [t131, t135]
blocked_by: []
---

## Goal

A `src/ui/main.py` (897 sor) render logikáját komponens fájlokba kell kiemelni. A `main.py` maradjon thin orchestration: page config, session init, layout, sidebar wiring.

## Scope

**Kiemelendő a `components/trade_panel.py`-ba:**
- `_render_active_trade_card()`
- `_render_recent_trades_panel()` + `_binance_trade_row_html()` + `_group_trades_by_minute()`
- `_render_signal_trigger_card()`
- `_render_trading_status_card()`
- `_render_trading_positions_card()`
- `_render_model_stats_panel()`
- `render_trade_panel()` (a publikus belépési pont)
- HTML konstansok: `_CARD`, `_LBL`, `_VAL`, `_HDR` (vagy `components/formatting.py`-ba, ha már létezik)

**Kiemelendő a `components/log_panel.py`-ba:**
- `_log_entries()`, `_split_log_header()`, `_terminal_line_html()`
- `_render_log_terminal()`
- `render_log_panel()` (`@st.fragment`)

**`main.py`-ban marad:**
- `st.set_page_config()`
- CSS inject
- Session state init
- `render_asset_chart()` (chart area, kisebb, maradhat)
- `_render_sync_controls()` + `_sync_panel_sol()` (sidebar sync)
- `_render_trading_controls()` (sidebar trading)
- Sidebar blokk
- Main layout (col_chart / col_trade)
- Palette konstansok (`_BG`, `_PANEL`, stb.) — közös helyen, vagy exportálva `components/` számára

**Már létező:**
- `src/ui/components/formatting.py` — megnézni, mi van benne, és a HTML konstansokat ide rakni ha logikus

## Acceptance Criteria

- [ ] `components/trade_panel.py` létezik, tartalmazza a fent listázott függvényeket
- [ ] `components/log_panel.py` létezik, tartalmazza a log rendering logikát
- [ ] `main.py` importálja a komponenseket és delegál
- [ ] `main.py` sor száma < 250
- [ ] A dashboard vizuálisan nem változik (smoke test: `STREAMLIT_CONFIG_DIR=src/ui uv run streamlit run src/ui/main.py`)
- [ ] `uv run pyright src/ui/` tisztán fut
- [ ] `uv run ruff check src/ui/ --fix` tisztán fut

## Notes

A `_fmt()` és `_fmt_time()` helper függvények a `trade_panel.py`-ban maradnak vagy `components/formatting.py`-ba kerülnek, ha az már tartalmaz hasonlókat.

A `_SOL_MODEL_STATS` modul-szintű betöltés (`_load_sol_model_stats()`) szintén átköltözik `trade_panel.py`-ba, mivel a `_render_model_stats_panel()` saját függvénye. A t134 task ezt fogja eltávolítani/simplifikálni — t130 csak áthelyezi.

A `_WINDOW_OPTIONS` és `render_asset_chart()` maradhat `main.py`-ban, mert a chart area thin marad.
