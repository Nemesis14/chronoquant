---
epic: epic_028
id: t125
title: Sync dependency fix — trading ne importáljon ui.sync-ből
assignee: ui_agent
status: todo
blocks: [t129]
blocked_by: []
---

## Goal

A `src/trading/live/service.py` jelenleg `from ui.sync import run_database_sync`-et hív a `_sync_data()` metódusban. Ez architectural violation: a trading layer az UI layertől függ.

Meg kell szüntetni ezt a coupling-ot úgy, hogy a trading service a `data_handling` sync funkciókat közvetlenül hívja.

## Scope

- `src/trading/live/service.py` — `_sync_data()` metódus átírása
- A sync orchestrációt (`sync_ohlcv` + `sync_features` + `sync_predictions` sorban) a trading service maga végezze el, a `data_handling.sync_tables` submodulokból importálva
- `src/ui/sync.py` nem változik (az UI-n belüli sync orchestrátor marad, saját lock-kezeléssel)

## Acceptance Criteria

- [ ] `src/trading/live/service.py` nem tartalmaz `from ui` importot
- [ ] `_sync_data()` direkt `data_handling.sync_tables` funkciókat hív
- [ ] A szinkronizálás sorrendje azonos marad: ohlcv → features → predictions
- [ ] Hiba esetén a service ugyanúgy logol és raise-el mint korábban
- [ ] `uv run pyright src/trading/` tisztán fut
- [ ] `uv run ruff check src/trading/ --fix` tisztán fut

## Notes

A `ui/sync.py`-ban lévő `get_sync_lock()` per-asset lock mechanizmus megmarad az UI-ban, mert a Streamlit background thread és a trading service közötti lock koordináció ott van megoldva. A trading service az `_sync_data()`-ban egyszerűen hívja a három sync funkciót sorban — a lock kezelés a `ui/sync.py`-ban az UI side-on megmarad (ha mindkettő fut egyszerre, a lock serializes them).

Ha a trading service és a Streamlit sync ugyanazt a lock-ot akarja használni, az `ui.sync.get_sync_lock()` importja megmarad CSAK a lock-ért — de ha ez zavaró, egyszerűbb megoldás: a trading service elfogadja, hogy nem lock-olja össze az UI szinkronnal (a DuckDB upsert idempotent).

[ui_agent] 2026-06-20
Megszuntetve az `ui.sync` fugges a `src/trading/live/service.py`-bol. A service most kozvetlenul `sync_ohlcv` -> `sync_features` -> `sync_predictions` sorrendben hivja a `data_handling.sync_tables` belepesi pontokat.
