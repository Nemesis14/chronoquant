---
id: t58
epic: epic_036
title: UI strategy info kártya
assignee: ui_agent
status: pr
blocks: [t60]
blocked_by: [t57]
---

## Context

Az UI jelenleg nem jeleníti meg az aktív stratégia kulcsparamétereit és teljesítményét.
A `backtest_summary()` fix (t57) után az artifact metrics elérhetők.

## Implementáció

`src/ui/main.py`-ba hozzáadva a `_render_strategy_card()` függvény, amely meghívja
`data.load_strategy_artifact()`-ot és megjeleníti:

**Fejléc sor:**
- Session ID + fit periódus (pl. `strat_solusdt_fw60_combo_2101_2605 | 2025-01 → 2026-05`)

**Paraméter sor** (4 oszlop, `st.markdown` HTML):
- Entry cutoff: `97%`
- TP spec: `0.75x_bucket_mean`
- SL spec: `none`
- Max hold: `60 min`

**Teljesítmény sor** (4 `st.metric` widget):
- Trades: `319`
- Win Rate: `63.3%`
- Compounded Return: `+49.3%`
- Avg Hold: `37.4 min`

**Graceful empty state:** ha `load_strategy_artifact()` üres dict-et ad vissza →
`st.info("Nincs aktív stratégia adat")`

**Elhelyezés:** `col_chart`-ban a predictions chart és a log panel között
(`render_asset_chart()` → `_render_strategy_card()` → `render_log_panel()`)

## Változtatott fájlok

- `src/ui/main.py` — `_render_strategy_card()` függvény hozzáadva + `_GREEN` import (ruff által eltávolítva, nem volt szükség rá)

## Ellenőrzések

- `ruff check src/ui/ --fix` — 1 auto-fix (unused import), 0 remaining error
- `uv run pyright src/ui/` — 0 errors, 0 warnings, 0 informations

## Notes

None required beyond acceptance checks above.
