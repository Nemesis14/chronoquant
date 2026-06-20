---
epic: epic_028
id: t128
title: Library docstring cleanup — trading/live/ fájlok
assignee: ui_agent
status: todo
blocks: [t129]
blocked_by: []
---

## Goal

A `src/trading/live/` library fájljaiban minden metóduson teljes `Args:` / `Returns:` docstring blokk van. A coding standard szerint: ne írj kommentet, ha a függvény neve és típusok önmagukért beszélnek. Max 1 soros comment, csak ha a WHY nem nyilvánvaló.

## Scope

- `src/trading/live/service.py`
- `src/trading/live/journal.py`
- `src/trading/live/exchange.py`
- `src/trading/live/state.py`
- `src/trading/live/strategy.py`

## Acceptance Criteria

- [ ] Minden `Args:` / `Returns:` docstring blokk eltávolítva
- [ ] Modul-szintű docstringek (fájl tetején) megtartva, ha értelmesek — max 2-3 sor
- [ ] Megtartandók: nem-nyilvánvaló WHY kommentek (pl. `# Binance Futures lot size for SOLUSDT: step 0.1 SOL`)
- [ ] Törlendők: nyilvánvaló leírások (pl. `"""Return True if the service has not been stopped."""`)
- [ ] `uv run ruff check src/trading/ --fix` tisztán fut
- [ ] `uv run pyright src/trading/` tisztán fut

## Notes

Példa a törlendő docstringre (`service.py:is_running()`):
```python
def is_running(self) -> bool:
    """Return True if the service has not been stopped.

    Returns:
        Boolean running status.
    """
```
Ez triviális — a típus és a név mindent elmond.

Megtartandó példa (`exchange.py`):
```python
# Binance Futures lot size for SOLUSDT perpetual: step 0.1 SOL
_SOL_QTY_STEP = 0.1
```
Ez nem nyilvánvaló business constraint.

A `# %% SectionName` Spyder/IPython cell markerek maradhatnak, ha az agent a projektben máshol is használja.

[ui_agent] 2026-06-20
Eltavolitottam a `src/trading/live/service.py`, `journal.py`, `exchange.py`, `state.py` es `strategy.py` fajlokbol a boilerplate `Args:/Returns:/Attributes:` docstring blokkokat. A modulfejlecek es a tenylegesen hasznos indoklo kommentek megmaradtak.
