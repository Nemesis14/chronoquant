---
epic: epic_018
id: t2
title: Feature engineering spec és _doc_ oldal
assignee: code_doc_agent
status: done
blocks: [t3, t4]
---

## Goal

A `src/modeling/feature_engineering/` modul dokumentálása: mi a célja, mik a lépések, mi az output formátuma. A spec egyrészt az analyst agentnek kell (t4), másrészt a code review alapja (t3).

## Scope

- `_doc_/modeling/feature_engineering.md` létrehozása
- `src/modeling/feature_engineering/` library fájlok áttekintése

## Acceptance Criteria

- [ ] `_doc_/modeling/feature_engineering.md` létezik és tartalmazza:
  - A 4 analízis lépés leírása (quality, target_relation, redundancy, stability)
  - Input: `quant_train` DuckDB tábla + `FeatureEngineeringConfig`
  - Output: `feature_set.json` (kiválasztott feature-ök listája) + `analyst_report.md`
  - `feature_set.json` sémája
  - Futtatási parancs: `uv run python src/modeling/01_feature_engineering.py --asset-id solusdt`
- [ ] A doc konzisztens az `_doc_/0000_project_overview.md`-vel

## Notes

A feature engineering library kód kész (`src/modeling/feature_engineering/`).
A `01_feature_engineering.py` script a modeling gyökerében van.
Output mappa: `database/solusdt/feature_engineering/<run_id>/`

**2026-06-19 — DONE:** `_doc_/modeling/feature_engineering.md` létrehozva.
Tartalmazza: 4 analízis lépés leírása, input/output spec, `feature_set.json` séma,
futtatási parancs, kapcsolódó fájlok. Konzisztens az `0000_project_overview.md`-vel.
Megjegyzés: a script `.ipynb` formátumú (nem `.py`), a futtatási parancs frissítve.
