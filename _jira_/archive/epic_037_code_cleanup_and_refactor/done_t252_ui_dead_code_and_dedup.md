---
epic: epic_037
id: t252
title: UI dead code removal + dedup cleanup
assignee: ui_agent
status: done
blocks: []
blocked_by: [t251]
---

## Goal

Halott kód eltávolítása és kisebb redundanciák megszüntetése a UI és trading rétegben.
t251 után futtatható (same agent, sequential).

## Scope

- `src/ui/components/trade_panel.py`
- `src/ui/components/charts.py`
- `src/ui/components/formatting.py`
- `src/ui/data.py`
- `src/ui/main.py`
- `src/ui/sync.py`
- `src/trading/live/service.py`
- `src/utils.py` (konstans hozzáadás)

## Acceptance Criteria

### 1. Halott függvények törlése

- [x] `_render_trading_status_card` (trade_panel.py:185, ~51 sor) — törölve
- [x] `_resolve_long_short_pred_cols` (data.py:483–491) — törölve
- [x] `equity_figure` (charts.py:418) — törölve
- [x] `import matplotlib.pyplot as plt` a charts.py tetején — törölve (az egyetlen matplotlib használat volt)

### 2. Color konstansok egységesítése

- [x] `formatting.py`-ban: `_GOLD`, `_GREEN`, `_RED`, `_MUTED`, `_PANEL`, `_TEXT`, `_GRID` — underscore eltávolítva (publikus konstansok: `GOLD`, `GREEN` stb.); `_BG` → `BG`
- [x] `charts.py`-ban: saját color definíciók törölve, `formatting.py`-ból importálva (alias-szal: `GOLD as _GOLD` stb.)
- [x] `main.py` import frissítve az új névekre
- [x] `log_panel.py` import frissítve
- [x] `trade_panel.py` import frissítve

### 3. `INITIAL_SYNC_START` konstans egységesítése

- [x] `utils.py`-ba kerül: `INITIAL_SYNC_START: str = "2017-01-01 00:00:00"`
- [x] `service.py` (`_INITIAL_SYNC_START`) és `sync.py` (`INITIAL_SYNC_START`) — mindkettő `utils.INITIAL_SYNC_START`-ot használ
- [x] Lokális definíciók törölve

### 4. `_next_open_time` / `_utc_str_to_ms` dedup

- [x] `utils.py`-ban `next_open_time(open_time: str) -> str` hozzáadva (pandas lazy import)
- [x] `service.py` és `sync.py` lokális `_next_open_time` / `_utc_str_to_ms` implementációk törölve
- [x] Mindkét hely `utils.next_open_time` / `utils.utc_str_to_ms`-t használ
- [x] `sync.py`-ból felesleges `datetime`, `UTC`, `timedelta` importok eltávolítva

### 5. `active_asset_id` config-driven

- [x] `main.py:306` (`active_asset_id = "solusdt"`) → `utils.load_asset_config(None)["database"]["asset_id"]`
- [x] `import utils` hozzáadva a `main.py`-hoz

### 6. Rank percentile dedup

- [x] `utils.py`-ban `apply_rank_percentile(series, lookup)` hozzáadva (numpy/pandas lazy import)
- [x] `data.py`-ban `_apply_rank_percentile` törlve; callerek `utils.apply_rank_percentile`-t hívnak
- [x] `TradingService._to_percentiles` — belső cached numpy tömbök maradtak (a metódus internal state-et kezel); csak a core logika egységesítés volt a cél

## Notes

Elvégezve 2026-06-23. Ruff: 5 automatikusan javított hiba, 0 megmaradó.

Pyright blocker: 13 hiba a `src/ui/data.py` és `src/trading/live/service.py` fájlokban,
de ezek kizárólag a t254 (code_doc_agent) által módosított `duckdb_query.py` `query_range`
visszatérési típusának megváltozásából (most `pd.DataFrame | pl.DataFrame`) erednek.
Ezek nem a t252 scope-jának részei — a megjegyzett sorok az EREDETI kódban is így szerepelnek.
A t252 módosításai (`utils.py` lazy import, `sync.py`, `service.py` dedup) nem okoznak pyright
hibát.

Blocker típusa: más agent (code_doc_agent t254) által bevitt type regression — validator_agent
feladata kezelni vagy `# type: ignore` kommentekkel semlegesíteni.
