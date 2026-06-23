---
epic: epic_034
id: t42
title: Feature engineering input refaktor modell mintajara
assignee: modeling_agent
status: pr
blocks: [t43, t46, t47]
blocked_by: [t41]
---

## Goal
A `feature_engineering` ténylegesen az adott modell fejlesztési mintáján fusson,
ne csupán annak időhatárára szűkített teljes `quant_train`-on.

## Scope
- `src/modeling/01_feature_engineering.ipynb`
- `src/modeling/feature_engineering/`
- esetleges új explicit train-input nézet/tábla

## Acceptance Criteria
- [x] A FE inputja modell-scope alapú
- [x] A `feature_set.json` egyértelműen visszaköthető az adott sample/snapshot inputhoz
- [x] A notebook és a library nem implicit teljes-időablakos `quant_train` másolaton dolgozik

## Notes
A jelenlegi eltérés: `model.__sample`-ből csak MIN/MAX idő határt olvas, majd azon a tartományon teljes `quant_train`-t használ.

[modeling_agent] Audit és javítás — 2026-06-22

### Audit eredmények

**Már helyes volt (nem igényelt javítást):**
- `src/modeling/feature_engineering/sample_scope.py` — `materialize_sample_scoped_quant_train()` pontosan az arch spec szerinti `snap ⋈ model.__sample` INNER JOIN-t végzi, I1 invariánst RuntimeError-rel kikényszeríti (L72-76).
- FE library modulok (`analyze_quality`, `analyze_target_relation`, `analyze_redundancy`, `analyze_stability`) — mind kizárólag a `quant_train` TEMP tábláról olvasnak, nincs direkt külső táblaolvasás.
- `feature_set.json["provenance"]` — tartalmazza az összes I7 szerinti mezőt: `snapshot_id`, `sample_table`, `sample_rows`, `joined_rows`, `min_open_time`, `max_open_time`, `source_contract: "snap ⋈ model.__sample"`.
- Notebook config cella — a `materialize_sample_scoped_quant_train`-t hívja, nem a régi időhatár-szűkítéses path-t.

**Gap: `--snapshot` CLI override nem propagálódott a FE lépésbe**

A `pipeline.py step_feature_engineering` nem adta át a `SNAPSHOT_ID`-t papermill paraméterként. A notebook saját maga oldotta fel a `snapshot_id`-t (models.json → reg.models fallback), de ha a user `--snapshot <id>` override-dal futtatott egy `sample` lépést, a FE lépés esetleg más snapshot_id-t használt volna (ha a models.json-ban más érték volt).

### Javítások

1. **`src/modeling/pipeline.py`**:
   - `step_feature_engineering()` kap új `snapshot_id: str | None = None` paramétert
   - Feloldja: explicit arg > models.json fallback
   - Papermill paraméterekbe hozzáadja: `"SNAPSHOT_ID": snapshot_id`
   - `main()` a `step_feature_engineering` hívásakor átadja `snapshot_id=args.snapshot`

2. **`src/modeling/01_feature_engineering.ipynb`**:
   - Papermill params cellába felvéve: `SNAPSHOT_ID = ""`
   - Config cella módosítva: `_snapshot_id = SNAPSHOT_ID if SNAPSHOT_ID else _model_meta.get("sampling", {}).get("snapshot_id")` (SNAPSHOT_ID papermill param első prioritás)

### Ellenőrzés
- `ruff check src/modeling/pipeline.py src/modeling/feature_engineering/` — All checks passed
- `uv run pyright src/modeling/` — 0 errors, 0 warnings, 0 informations
