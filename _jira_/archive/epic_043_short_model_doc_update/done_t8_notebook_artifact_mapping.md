---
epic: epic_043
id: t8
title: Notebook → artifact mapping (.ipynb fájlok elhelyezése)
assignee: analyst_agent
status: pr
blocks: []
blocked_by: [t1]
---

## Goal

Azonosítani és elhelyezni a session során keletkezett `.ipynb` fájlokat a megfelelő artifact mappákba.

## Scope

Ellenőrzendő notebook-ok:
- `src/modeling/01_feature_engineering.ipynb` — long FE futtatás eredménye → `artifacts/lgbm_solusdt_l_fw60_2101_2605/feature_engineering/`
- `src/modeling/01_feature_engineering.ipynb` — short FE futtatás eredménye → `artifacts/lgbm_solusdt_s_fw60_2101_2605/feature_engineering/`
- Bármilyen más `.ipynb` amit a session során futtattak

## Feladat

1. Listáld a `src/modeling/`, `analyst/`, `artifacts/` mappák `.ipynb` fájljait
2. Ellenőrizd melyik artifact mappában hiányzik a notebook
3. Másold/linkeld a megfelelő helyre
4. Az artifact `manifest.json`-ban rögzítsd ha szükséges

## Acceptance Criteria

- [x] Long FE notebook elérhető az artifact mappájában
- [x] Short FE notebook elérhető az artifact mappájában
- [x] Nincs orphan notebook (létezik de nincs artifact-hoz kötve)

## Notes

### Elvégzett munka

**Ellenőrzés eredménye:**

1. `artifacts/lgbm_solusdt_l_fw60_2101_2605/feature_engineering/` — tartalmaz:
   - `01_feature_engineering.ipynb` ✓
   - `01_feature_engineering.html` ✓
   - `feature_set.json` ✓

2. `artifacts/lgbm_solusdt_s_fw60_2101_2605/feature_engineering/` — tartalmaz:
   - `01_feature_engineering.ipynb` ✓
   - `01_feature_engineering.html` ✓
   - `feature_set.json` ✓

Mindkét notebook már a megfelelő helyen volt — nem kellett másolni.

**Manifest frissítés:**

A short modell `manifest.json` hiányos volt (csak 3 mezőt tartalmazott: `provenance`, `pipeline_status`, `updated_at`). Frissítve a long modell struktúrájával összhangban:
- `model_id`: `lgbm_solusdt_s_fw60_2101_2605`
- `display_name`: "SOL Short LightGBM Champion 2101-2605"
- `description`: töltve
- `asset_id`: `solusdt`
- `target_name`: `short_mfe_fw60`
- `family`: `lightgbm`
- `trainer`: `lightgbm_regression`
- `sampling`: azonos snapshot és CV konfiguráció mint a long modell
- `created_at`: `2026-06-28T07:25:30.000000+00:00` (feature_set.json `created_at` alapján)

**Orphan notebook ellenőrzés:**
- `src/modeling/search/search_report.ipynb` — search template/source, nem orphan (a search könyvtárak artifact-okban tartalmazzák a saját példányaikat)
- `control.ipynb` — gyökérszintű vezérlő notebook, helyén van
- `_doc_/models_doc/` notebookok — analyst_agent dokumentációs zóna, rendben
