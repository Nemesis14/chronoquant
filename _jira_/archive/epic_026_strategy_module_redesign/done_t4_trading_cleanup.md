---
epic: epic_026
id: t4
title: src/trading/ calibration kód eltávolítása
assignee: ui_agent
status: pr
blocks: [t5]
blocked_by: []
---

## Goal

A `src/trading/` modulból eltávolítani minden strategy calibration logikát.
Az eltávolítás már megtörtént az orchestrátor által — ez a task a maradék cleanup-ot és
a `02_run_service.py` integritásának ellenőrzését végzi.

## Scope

### Már törölt (orchestrátor által)

- `src/trading/calibration/` (teljes könyvtár: `__init__.py`, `artifacts.py`, `backtest.py`, `calibrate.py`)
- `src/trading/00_calibrate_strategy.py`
- `src/trading/01_sweep_strategy.py`

### Ellenőrizni / elvégezni

- `src/trading/__init__.py` — nincs-e import calibration modulra
- `src/trading/tests/` — calibrationre hivatkozó tesztek törlése
- `src/trading/live/` — nincs-e dependency a törölt modulra
- `src/trading/02_run_service.py` — fut-e hiba nélkül (import check)
- `_doc_/` — van-e calibration-ra hivatkozó dokumentáció, amit frissíteni kell

## Acceptance Criteria

- [ ] `src/trading/` importjai tiszták (nincs broken import)
- [ ] `uv run python -c "import src.trading.live"` hiba nélkül fut
- [ ] `ruff check src/trading/` tiszta
- [ ] `pyright src/trading/` tiszta
- [ ] Calibrationre utaló tesztek eltávolítva

## Notes

### Elvégzett munka (2026-06-20)

**Törölt fájlok:**
- `src/trading/tests/calibration/` — teljes könyvtár (3 tesztfájl, `__init__.py`-k, `__pycache__`)

**Módosított Python fájlok:**
- `src/trading/__init__.py` — docstring frissítve (calibration eltávolítva)
- `src/trading/live/strategy.py` — docstring comment javítva (`trading.calibration.backtest` → `src/strategy/`)
- `src/trading/live/service.py` — docstring comment javítva
- `src/trading/tests/live/smoke/test_state.py` — import sorrend javítva (ruff E402, F401)
- `src/trading/tests/live/smoke/test_strategy.py` — import sorrend javítva (ruff E402, F401)

**Módosított doc fájlok:**
- `_doc_/0000_project_overview.md` — calibration hivatkozások frissítve (`src/strategy/`-ra mutatnak)
- `_doc_/6000_trading.md` — modul struktúra, adatfolyam ábra, entry point tábla frissítve
- `_doc_/6100_calibration.md` — ARCHIVÁLT megjegyzés hozzáadva

**Ellenőrzések:**
- `uv run python -c "import src.trading.live"` — OK
- `uv run ruff check src/trading/` — 0 hiba
- `uv run pyright src/trading/` — 0 hiba, 0 warning
