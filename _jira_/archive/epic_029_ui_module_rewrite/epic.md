---
epic: epic_029
title: UI module rewrite — coding standard alignment + stale logic cleanup
status: active
---

## Cél

A `src/ui/` modul az új coding standardre és a megváltozott backend (strategy artifact, trading journal) kontrakcióra hozása:

1. `main.py` szétbontása — 897 soros God file render logikája komponensekbe kerül
2. Strategy API frissítés — `load_long_short_strategies()` az új `strategy_artifact.json`-t olvassa
3. `active_position()` fix — rossz DB-ből olvas (solusdt.duckdb helyett trading.db)
4. Stale backtest path-ek cleanup — régi `backtests/solusdt_long_fw60_q90_local_v3/` referenciák
5. Model stats cleanup — `model_card.json` (nem létezik) + stale metrikák (AUC vs regresszió)
6. Signal trigger card frissítés — raw score vs threshold helyett journal döntés
7. `data.py` comment block cleanup — `# ===...===` wrapper blokkok eltávolítása

## Motiváció

Az UI több, egymástól független rétegben maradt el a backend változásoktól:
- A strategy modul átállt artifact-alapú, percentile-first contrakcióra — a dashboard még a régi `entry_threshold` mezőt keresi
- A trading journal `trading.db`-ben él, de a dashboard `solusdt.duckdb`-ből próbálja kiolvasni a pozíciókat
- `main.py` karbantarthatatlanul nagy; a render komponensek szétbontás nélkül nem fejleszthetők izoláltan

## Taskok

| ID | Cím | Assignee | Blokkolja | Blokkolt általa |
|----|-----|----------|-----------|-----------------|
| t130 | main.py szétbontás komponensekre | ui_agent | t131, t135 | — |
| t131 | Strategy API frissítés | ui_agent | t130 | t137 |
| t132 | active_position() DB fix | ui_agent | — | t137 |
| t133 | Stale backtest paths cleanup | ui_agent | — | t137 |
| t134 | Model stats cleanup | ui_agent | — | t137 |
| t135 | Signal trigger card frissítés | ui_agent | t130 | t137 |
| t136 | data.py comment cleanup | ui_agent | — | t137 |
| t137 | Validator session | validator_agent | t130–t136 | — |
