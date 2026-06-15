---
epic: epic_006
id: t4
title: Import path-ok frissítése az összes .py fájlban
assignee: database_agent
status: todo
blocked_by: [t1, t2, t3]
blocks: [t5]
---

## Goal
Az összes Python fájlban frissíteni az import path-okat az új `src/` struktúrának megfelelően. A kód t1-t3 után broken — ez a task állítja helyre.

## Scope

**Mapping (régi → új):**
```
from store import X          → from database.store import X
from data_pipeline import X  → from database.data_pipeline import X
from modeling import X       → from modeling.quantitative import X
from evaluation import X     → from modeling.quantitative.evaluation import X
from elliott_waves import X  → from modeling.elliott import X
from streamlit_app import X  → from ui import X
```

**Érintett fájlok (minden .py):**
- `src/database/**/*.py`
- `src/modeling/**/*.py`
- `src/trading/**/*.py`
- `src/ui/**/*.py`
- `src/utils.py`

**sys.path beállítások a scriptekben** (`src/database/0X_*.py`, `src/modeling/quantitative/0X_*.py`, stb.) szintén frissítendők.

**pyproject.toml / conftest.py** — ellenőrizni és frissíteni ha szükséges.

## Acceptance Criteria
- [ ] `uv run pyright src/` hibamentes (import szempontból)
- [ ] `ruff check src/ --fix` tiszta
- [ ] `uv run pytest _tests/ -v` átmegy
- [ ] Nincs régi path hivatkozás (`from store import`, `from modeling import` stb.) a codebase-ben

## Notes
Grep-pel azonosítani az összes érintett helyet mielőtt módosítás történik.
Ha az `__init__.py` fájlok re-exportálnak valamit, azokat is frissíteni kell.
