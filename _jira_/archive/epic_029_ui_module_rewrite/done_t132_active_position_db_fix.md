---
epic: epic_029
id: t132
title: active_position() DB fix — trading.db-ből olvasás
assignee: ui_agent
status: done
blocks: [t137]
blocked_by: []
---

## Goal

A `data.active_position()` jelenleg az asset DuckDB-ből (`solusdt.duckdb`) próbálja olvasni a `trading_positions` táblát. Ott nem létezik ez a tábla — a trading journal `trading.db`-ben él. Ezért az active position sosem jelenik meg a dashboardon (a chart overlay és a trade card is üres marad).

## Scope

- `src/ui/data.py` — `active_position()` metódus

## Acceptance Criteria

- [ ] `active_position()` a `trading.db`-ből olvassa a `trading_positions` táblát
- [ ] A DB path forrása: `utils.load_trading_config()["db_path"]` (ugyanaz mint `journal.trading_db_path()`)
- [ ] Ha `trading.db` nem létezik, graceful return: `None`
- [ ] Ha nincs nyitott pozíció, return: `None`
- [ ] A chart overlay aktív pozíció esetén megjelenik
- [ ] `uv run pyright src/ui/data.py` tisztán fut

## Notes

A `_db_path()` helper jelenleg az asset konfigból adja vissza a DuckDB path-t (solusdt.duckdb), fallback trading config. Ez a fallback logika volt a szándék, de nem működik mert az asset config db_path mindig van, és az asset duckdb-t adja vissza.

Legegyszerűbb fix: `active_position()` ne a generikus `_db_path()`-t hívja, hanem direkt `utils.load_trading_config()["db_path"]`-t — ugyanúgy ahogy `trading_runner.py` teszi.

Opcionálisan: importálni `from trading.live.journal import get_open_position, trading_db_path` és azt hívni, hogy ne duplikálódjon a query logika.
