---
epic: epic_028
id: t126
title: Script fix + rename — 02_run_service.py javítás
assignee: ui_agent
status: todo
blocks: [t127, t129]
blocked_by: []
---

## Goal

A `src/trading/02_run_service.py` két hibát tartalmaz:

1. **Dupla `_startup()` bug:** A CLI script meghívja `service._startup()`, majd `service._run()`-t — de `_run()` maga is elsőként hívja `_startup()`-ot. Ez dupla inicializációt okoz (két `run_id`, dupla DB setup).
2. **Stale docstring:** A modul docstringje `python scripts/run_trading_service.py`-t ír, holott a helyes path `src/trading/02_run_service.py` (vagy `01_run_service.py` átnevezés után).
3. **Script numbering:** Nincs `00_` és `01_` script a trading modulban, a `02_` szám lóg. Amennyiben ez az egyetlen script, `01_run_service.py`-ra kell átnevezni. A project overview is frissítendő.

## Scope

- `src/trading/02_run_service.py` → `src/trading/01_run_service.py` (rename)
- Fix: `service._startup(); service._run()` → csak `service._run()` (a `_run()` maga hívja `_startup()`-ot)
- Docstring frissítése: helyes path, helyes usage
- `_doc_/0000_project_overview.md`: `02_run_service.py` → `01_run_service.py` az repository layout szekcióban

## Acceptance Criteria

- [ ] `01_run_service.py` létezik, `02_run_service.py` törlve
- [ ] CLI futtatva nem hívódik duplán `_startup()` (egy `run_id` keletkezik)
- [ ] Docstring helyes path-t mutat
- [ ] Project overview frissítve
- [ ] `uv run python src/trading/01_run_service.py --mode dry_run` elindul hiba nélkül

## Notes

A `main()` függvényben jelenleg:
```python
service._startup()
service._run()
```
Ez helyett:
```python
service._run()  # _run() calls _startup() internally
```

A `sys.path.insert(0, ...)` sor is vizsgálandó: ha a project `uv run python src/trading/01_run_service.py` módban fut (src a pythonpath-ban), nem kell. Ha standalone futtatáshoz szükséges, maradhat.

[ui_agent] 2026-06-20
`src/trading/02_run_service.py` atnevezve `src/trading/01_run_service.py` nevra. A CLI docstring frissitve lett, es a dupla inicializacio megszunt: a script mar csak `service._run()`-t hiv.
