---
epic: epic_072
id: t916
title: Vedd fel a torch és tslearn függőségeket
assignee: code_doc_agent
status: done
blocks: [t919]
blocked_by: []
---

## Goal
CPU `torch` (autoencoder embedding) és `tslearn` (k-Shape / DTW k-means) felvétele `uv`-vel, opcionális
dependency-csoportban (pl. `research`), hogy a fő runtime ne nehezedjen.

## Acceptance Criteria
- [ ] `pyproject.toml` + lock frissítve, opcionális csoport
- [ ] Import smoke teszt
- [ ] Dokumentálva a `_doc_/database_and_code_doc/` releváns helyén

## Notes
- **Módosított fájlok:** `pyproject.toml` (új `[dependency-groups] research` = `torch`, `tslearn`; `[[tool.uv.index]] pytorch-cpu` = `https://download.pytorch.org/whl/cpu`, `explicit = true`; `[tool.uv.sources] torch = { index = "pytorch-cpu", group = "research" }`), `uv.lock` (`uv lock`, +17 csomag, 0 eltávolított/verzióváltott meglévő csomag), új doc `_doc_/database_and_code_doc/0005_tooling_dependencies.md`.
- **Verziók:** torch `2.15.0+cpu` (CPU build, `cuda` nincs), tslearn `0.10.0`, numba `0.68.1`, statsmodels `0.15.0`. Python 3.12.2, uv 0.9.26. Windows cp312 CPU wheel elérhető volt.
- **Parancsok:** `uv lock`, `uv sync --group research`, `uv run --group research python -c "import torch, tslearn; print(torch.__version__, tslearn.__version__)"`.
- **Smoke:** `2.15.0+cpu 0.10.0`, exit 0 (kétszer: research sync után és default sync után).
- **Fő runtime:** `uv lock --check` OK; `uv sync` (default) eltávolítja a torch/tslearn csomagokat (`torch present: False`, `tslearn present: False`) — a fő runtime nem nehezül; `uv run pytest src/reports -q` → 42 passed.
- **Megjegyzés:** a research sync eltávolított két nem-lockolt, nem-deklarált venv csomagot (`matplotlib 3.10.9`, `kiwisolver 1.5.0`); `src/` nem importálja őket. Ha egy notebook/script mégis igényli, `dev` csoportba kell venni.
- **Lock:** a torch/numba miatt a resolution-marker blokkok darwin / Python 3.13 ágakkal bővültek; meglévő verziók nem változtak.
- Ticket neve nem változott (`todo_`), státusz a validator/orchestrátor feladata.

[validator] Done — 2026-11-20
- `uv lock --check` → OK (106 csomag, exit 0); a lock-diff csak resolution-marker sorokat cserél, meglévő csomagverzió nem változott.
- `uv run --group research python -c "import torch, tslearn"` → `2.15.0+cpu 0.10.0`, `cuda.is_available()=False`, exit 0.
- Alap futás: `uv export --frozen` (default csoportok) nem tartalmaz torch/tslearn/numba-t; `src/` nem importálja őket; `uv run pytest src/reports` research nélkül is fut.
- Doc `0005_tooling_dependencies.md` egyezik a `pyproject.toml`-lal, a `0000_project_overview.md` hivatkozza.
