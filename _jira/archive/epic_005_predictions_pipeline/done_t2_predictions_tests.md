---
epic: epic_005
id: t2
title: Predictions perf tesztek + integration teszt + összes teszt futtatása
assignee: validator_agent
status: done
---

## Goal
A predictions táblához hiányoznak a perf tesztek és az integration teszt.
Meg kell írni őket, majd le kell futtatni az összes tesztet.

## Scope
- `_tests/store/perf/test_query_timing.py` — predictions szekció hozzáadása
- `_tests/data_pipeline/integration/test_pipeline_integration.py` — új fájl (üres könyvtár)
- `_tests/` — összes teszt futtatása (smoke + sanity + perf)

## Acceptance Criteria
- [ ] `test_query_timing.py`-ban van predictions COUNT(*), range 7d/30d, GROUP BY year/month
- [ ] `test_pipeline_integration.py` létezik és tesztel ohlcv → features → target → predictions cross-layer flowt
- [ ] `uv run pytest _tests/ -v` hibamentesen lefut (vagy csak a DB-hiány miatt skip-el)
- [ ] Ruff és pyright clean az érintett fájlokon

## Notes
Meglévő tesztek (nem kell megírni, már léteznek):
- `_tests/data_pipeline/smoke/test_sync_predictions.py` (smoke)
- `_tests/store/sanity/test_predictions.py` (sanity)

Validator agent rules: perf tesztek assertions: COUNT(*) < 2s, range < 3-5s, GROUP BY < 3s.
Integration teszt: `_tests/data_pipeline/integration/` könyvtár, `pytestmark = pytest.mark.integration`.

[validator] Elvégezve — 2026-06-14
- `test_query_timing.py`: predictions szekció hozzáadva (COUNT, range 7d/30d, GROUP BY year/month)
- `_tests/data_pipeline/integration/test_pipeline_integration.py`: új fájl, 3 teszt (cross-layer alignment, close match, target subset)
- `uv run pytest _tests/ -v`: 90/90 passed
- Ruff és pyright clean
