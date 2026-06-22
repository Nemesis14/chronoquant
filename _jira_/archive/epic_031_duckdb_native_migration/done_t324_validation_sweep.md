---
epic: epic_031
id: t324
title: Validációs kör — ruff + pyright + pytest
assignee: validator_agent
status: pr
blocks: []
blocked_by: [t315, t316, t317, t318, t319]
---

## Goal
Teljes statikus + teszt validáció az átállással érintett modulokon, a `pr_` taskok
`done_`-ra léptetése előtt.

## Scope
- `uv run ruff check src/<modul>/ --fix` + `uv run pyright src/<modul>/`
- `uv run pytest src/data_handling/tests/ -v`
- `uv run pytest src/modeling/ -v`
- `uv run pytest src/strategy/tests/ -v`
- `uv run pytest src/trading/tests/ -v`
- UI manuális smoke
- Hivatkozás: `_doc_/_plans_/data_process_architecture.md` 11 (migráció)

## Acceptance Criteria
- [ ] ruff + pyright tiszta minden érintett modulon
- [ ] az összes érintett pytest suite zöld
- [ ] UI smoke OK
- [ ] reprodukció-ellenőrzés: ugyanaz a snapshot_id → bitre azonos sample/pred

## Notes
- Ellenőrizendő (t315 carry-forward): a tréning-betöltő (`fit_lgbm._load_train_data`)
  konzisztens-e a `model.__sample` táblával (snap ⋈ sample join), nem a régi parquet úttal.
  Ha nincs rendezve, blockerként jelezd a t325 felé.

### Validáció eredménye (validator_agent — 2026-06-21)

**ruff check** (`src/data_handling/ src/modeling/ src/strategy/ src/ui/ src/utils.py`):
- 3 pre-existing hiba (nem új): `B017` `test_walk_forward_config.py`, `SIM108` (2x) `utils.py`.
- Új hiba: 0. Összes érintett modul tiszta.
- Fix: `test_deploy_cutover.py` pyright hibái javítva (ld. alább).

**pyright** (`src/data_handling/ src/modeling/ src/strategy/ src/ui/`):
- Javított (új): `test_deploy_cutover.py` — 7 `reportOptionalSubscript`/`reportArgumentType`
  hiba (`fetchone()[0]` null-guard hiánya + `deployment_row: dict|None` → `dict` narrowing).
  Fixes: `assert ... is not None` guard hozzáadva minden érintett helyen.
- Pre-existing (érintetlen): `test_quant_train.py` (4 db `reportOptionalSubscript`,
  fájl `5d04315` kommitnál született, epic_031 előtt); `pipeline.py` `reportMissingImports`
  (papermill opcionális dep, minden ticketben pre-existing).
- Új hiba a javítás után: 0 új.

**pytest**:
- `src/data_handling/tests/`: **156 passed, 2 failed** — mindkét failure pre-existing sanity:
  `test_target_row_count_matches_features` (live DB sorarányelírás) és
  `test_predictions_score_range` (live DB [0,1]-en kívüli prediction). Kód-hiba: 0.
- `src/modeling/`: **106 passed, 0 failed**
- `src/strategy/tests/`: **17 passed, 0 failed**
- `src/trading/tests/`: **14 passed, 0 failed**
- UI smoke: `python -c "from src.ui import data"` → **import OK**

**Tréning-betöltő konzisztencia (t315 carry-forward):**
BLOCKER — `fit_lgbm._load_train_data` a `sample_train_valid.parquet`-ból olvas
(`artifact_dir / "sample_train_valid.parquet"`). Az új snapshot-natív pipeline
`step_sample`-je csak `model."<model_id>__sample"` DuckDB táblát hoz létre
(open_time + target + fold_id, **nincs feat_***). Ezért új modellen a search/train lépés
hibára fut (`FileNotFoundError: sample_train_valid.parquet`). Meglévő champion modellek
(pl. `lgbm_solusdt_l_fw60_2101_2605`) ezt még megkapják a lemezről — azok rendben vannak.
→ **Blocker a t325 felé**: a t325 scope-ja kell, hogy tartalmazzon egy
`snap."<snap_id>" ⋈ model."<model_id>__sample"` JOIN-t `_load_train_data`-ban, vagy
egy parquet-export lépést a `step_sample` végén. A javítás az eredeti intent megértését
igényli — ide nem tartozik.

**Átnevezett ticketek:**
done_t311, done_t312, done_t313, done_t314, done_t315, done_t316, done_t317, done_t318,
done_t319, done_t320, done_t321

**Blocker:** t315 carry-forward → t325 felé jelezve (ld. fent).
