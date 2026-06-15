---
epic: epic_006
id: t3
title: Trading és UI domain — fájlok mozgatása, simulation mappa, script számozás
assignee: ui_agent
status: todo
blocks: [t4]
---

## Goal
A `src/trading/` alá létrehozni a `simulation/` almappát a strategy-szintű backtest logikának. A `src/streamlit_app/` tartalmat átköltöztetni `src/ui/`-ba. A trading script számozva a `src/trading/` gyökerébe.

## Scope

**Trading:**
- `src/trading/simulation/` mappa létrehozása (egyelőre üres — a strategy backtest logika ide kerül majd)
- `scripts/trading/run_trading_service.py` → `src/trading/01_run_service.py`
- A többi trading fájl (`exchange.py`, `journal.py`, `strategy.py`, `state.py`, `service.py`) marad `src/trading/` alatt

**UI:**
- `src/streamlit_app/*` → `src/ui/` (minden fájl és almappa direktbe)
- `src/streamlit_app/` törölt

**NE frissítsd az import path-okat** — az t4 feladata.

## Acceptance Criteria
- [ ] `src/trading/simulation/` mappa létezik (üres `__init__.py`-val)
- [ ] `src/trading/01_run_service.py` megvan
- [ ] `src/ui/` tartalmazza a jelenlegi `streamlit_app/` fájlokat
- [ ] `src/streamlit_app/` törölt
- [ ] `scripts/trading/` törölt

## Notes
Import path-ok szándékosan érintetlenek — a kód t4-ig broken állapotban lesz.
