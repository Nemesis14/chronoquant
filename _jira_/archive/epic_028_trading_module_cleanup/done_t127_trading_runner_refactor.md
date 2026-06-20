---
epic: epic_028
id: t127
title: trading_runner refactor — publikus service API használata
assignee: ui_agent
status: todo
blocks: [t129]
blocked_by: [t126]
---

## Goal

A `src/ui/trading_runner.py` jelenleg a `TradingService` privát `_run()` metódusát hívja közvetlenül, saját thread-et spawnolja. Ez megkerüli a `service.start()` publikus API-t, és duplikálja a thread management logikát.

Át kell írni úgy, hogy `service.start()` és `service.stop()` / `service.is_running()` publikus metódusokat használjon.

## Scope

- `src/ui/trading_runner.py` — `start_trading()`, `is_trading_running()`, `stop_trading()`

## Acceptance Criteria

- [ ] `start_trading()` hívja `service.start()` (nem spawol saját thread-et)
- [ ] `is_trading_running()` delegál `service.is_running()`-ra (nem tartja saját `_service_thread` handle-t)
- [ ] `stop_trading()` hívja `service.stop()` (változatlan)
- [ ] A modul-szintű `_service_thread` változó eltávolítható (ha `service.is_running()` elegendő)
- [ ] A Streamlit UI-ban a trading start/stop/status nem változik funkcionálisan
- [ ] `uv run pyright src/ui/` tisztán fut

## Notes

A `_service_instance` modul-szintű singleton megmarad — a Streamlit rerenderek között ez tartja életben a service-t. Csak a thread management változik: `start()` belsőleg kezeli a thread-et, `is_running()` a `_stop_event`-en alapul.

Ha `service.is_running()` nem elég (pl. thread crash esetén a `_stop_event` még nincs set), opcionálisan megmaradhat egy `_service_thread` referencia a `is_trading_running()` implementációjában — de ekkor is `service.start()` hívja, nem a runner spawnolja saját maga.

[ui_agent] 2026-06-20
`src/ui/trading_runner.py` atallt a publikus `TradingService.start()/stop()/is_running()` API-ra. Ezzel egyutt a `TradingService` sajat thread-referenciat tart fenn, igy az `is_running()` a tenyleges worker thread eletciklusat jelzi.
