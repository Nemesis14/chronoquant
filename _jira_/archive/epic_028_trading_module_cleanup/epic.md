---
epic: epic_028
title: Trading module cleanup — coding standard alignment
status: active
---

## Cél

A `src/trading/` modul az új coding standardre hozása:

1. Architectural fix: a trading service ne importáljon `ui.sync`-ből (rossz irányú coupling)
2. Script fix: dupla `_startup()` bug javítása, script renaming
3. `trading_runner.py`: service privát API helyett publikus API használata
4. Library fájlok docstring cleanup a coding standard szerint

## Motiváció

A trading modul az épülő strategy és modell rewrite után mögötte maradt:
- `service.py` `ui.sync`-et importál → trading layer függ az UI-tól (architectural violation)
- `02_run_service.py` hibás startup sequencecel rendelkezik (dupla `_startup()`)
- `trading_runner.py` privát `_run()` metódust hív közvetlenül, megkerülve `service.start()`-ot
- Library fájlokban excessive Args:/Returns: docstringek (minden metóduson), ami ütközik a coding standarddel

## Taskok

| ID | Cím | Assignee | Blokkolja | Blokkolt általa |
|----|-----|----------|-----------|-----------------|
| t125 | Sync dependency fix | ui_agent | — | t129 |
| t126 | Script fix + rename | ui_agent | — | t127, t129 |
| t127 | trading_runner refactor | ui_agent | t126 | t129 |
| t128 | Library docstring cleanup | ui_agent | — | t129 |
| t129 | Validator session | validator_agent | t125, t126, t127, t128 | — |
