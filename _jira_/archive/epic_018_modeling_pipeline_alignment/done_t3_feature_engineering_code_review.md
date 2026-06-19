---
epic: epic_018
id: t3
title: Feature engineering kód review és teszt
assignee: modeling_agent
status: done
blocked_by: [t2]
blocks: [t8]
---

## Goal

A `src/modeling/feature_engineering/` library kódjának átvizsgálása a t2-ben elkészült spec alapján. Ellenőrizni, hogy a kód megfelel a specifikációnak, minőségileg rendben van, és a tesztek átmennek.

## Scope

- `src/modeling/feature_engineering/config.py`
- `src/modeling/feature_engineering/quality.py`
- `src/modeling/feature_engineering/target_relation.py`
- `src/modeling/feature_engineering/redundancy.py`
- `src/modeling/feature_engineering/stability.py`
- `src/modeling/feature_engineering/reporting.py`
- `src/modeling/feature_engineering/__init__.py`
- `src/modeling/feature_engineering/tests/`

## Acceptance Criteria

- [ ] Kód konzisztens a t2-ben rögzített specccel
- [ ] `feature_set.json` output sémája helyes
- [ ] `uv run pytest src/modeling/feature_engineering/tests/ -v` átmegy
- [ ] `ruff check src/modeling/feature_engineering/ --fix` tiszta
- [ ] `uv run pyright src/modeling/feature_engineering/` tiszta

## Notes

Ha a review során spec-eltérés kerül elő, a t2 ticketet kell frissíteni (Notes szekció), nem a kódot módosítani spec nélkül.

**2026-06-19 — DONE:**
- `ruff check src/modeling/feature_engineering/` → All checks passed
- `pyright src/modeling/feature_engineering/` → 0 errors (fixture return type javítva: `Iterator[duckdb.DuckDBPyConnection]`)
- `pytest src/modeling/feature_engineering/tests/ -v` → 17/17 PASSED
- Kód konzisztens a t2 speccel: 4 analízis lépés, helyes output séma, DuckDB alapú számítások Polars outputtal
- Spec-eltérés: a script `.ipynb` (nem `.py`), de ez elfogadható — t2 Notes frissítve
