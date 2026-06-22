---
epic: epic_031
id: t318
title: Cleanup + strategies.json normalizálás + registry-validátor
assignee: database_agent
status: todo
blocks: [t324]
blocked_by: [t313, t316]
---

## Goal
Holt/ellentmondó kódutak eltakarítása (P5) és a `config/strategies.json` normalizálása
a registryhez igazítva (P3), plusz egy validátor, ami jelzi, ha egy config elcsúszik a registrytől.

## Scope
- `src/data_handling/store/duckdb_store.py` — `materialize_sample_table` eltávolítása
- `ensure_tables()` inline migrációk → migrations.py (t311) használata
- `config/strategies.json` — egységes asset_id/model_id séma; elavult bejegyzések tisztítása
- registry-validátor: config ↔ reg.* konzisztencia ellenőrzés
- Hivatkozás: `_doc_/_plans_/data_process_architecture.md` 2.2 (P3, P5), 4.3

## Acceptance Criteria
- [ ] materialize_sample_table törölve, hivatkozásai megszűntek
- [ ] ensure_tables nem tartalmaz inline ALTER/DROP migrációt
- [ ] strategies.json konzisztens a models.json/assets.json sémával
- [ ] validátor jelzi a config↔registry eltérést
- [ ] érintett tesztek zöldek; ruff + pyright tiszta

## Notes

### Elvégezve (database_agent)

**Módosított fájlok:**
- `src/data_handling/store/duckdb_store.py` — `materialize_sample_table` eltávolítva; `ensure_tables` átírva migrations framework-re.
- `src/data_handling/tests/store/smoke/test_validate.py` — `materialize_sample_table` import + hívás eltávolítva; `_seed_sample` helper inline DuckDB CREATE-re állítva.
- `config/strategies.json` — normalizálva (schema_version 2, asset_id + model_id igazítva).

**Törölt fájlok:**
- `src/data_handling/tests/store/smoke/test_materialize_sample.py` — a függvény megszűnése miatt feleslegessé vált.

**Létrehozott fájlok:**
- `src/data_handling/store/registry_validator.py` — registry validátor (lásd alább).

---

**1. materialize_sample_table törlés:**
- A függvény `duckdb_store.py`-ból teljesen eltávolítva (P5 dead code — t313 óta a `model."<id>__sample"` DuckDB tábla a szampling output, nem `sample_*` tábla a live DB-ben).
- Hívók a `src/`-ban: csak `test_materialize_sample.py` (törölt) és `test_validate.py` (frissítve).
- Más hívó a `src/`-ban nem volt.

**2. ensure_tables inline migrációk → migrations.py:**
- Az `ensure_tables` korábbi inline ALTER/DROP/CREATE logic 5 `Migration` objektumba szervezett: v1 (core táblák), v2 (boolean target séma drop), v3 (legacy split col drop), v4 (boolean predictions target drop + fw60 add), v5 (model stamp cols).
- Az `ensure_tables` most csak `run_migrations(conn, LIVE_DB_MIGRATIONS)` hívja.
- A `LIVE_DB_MIGRATIONS` lista tetején doc: mindig append, soha ne törlés.
- Import: `from data_handling.store.migrations import Migration, run_migrations`.

**3. strategies.json normalizálás:**
- `schema_version` 1 → 2.
- Minden `"asset_id": "solusdt_fw60"` → `"asset_id": "solusdt"` (assets.json-ban `solusdt` a kulcs, `features_profile: "solusdt_fw60"`).
- `model_id` mezők a két champion felé normalizálva: `lgbm_solusdt_l_fw60_2101_2605` (long stratégiák) és `lgbm_solusdt_s_fw60_2101_2605` (short stratégiák). Eredeti régi model_id-k (`q90_local_v4`, `q10_local_v4`, stb.) megszűntek — a strategy_key-ek (`solusdt_long_fw60_q90_local_v4`, stb.) megmaradnak (backtest-azonosítók). A t326 végzi el a felesleges entry-purge-t.

**4. registry_validator.py — ValidatorResult + validate_registry():**
- `src/data_handling/store/registry_validator.py` (új fájl)
- `ValidationResult` dataclass: `ok: bool`, `missing_models: list[str]`, `missing_strategies: list[str]`, `model_id_mismatches: list[dict]`, `asset_id_mismatches: list[dict]`.
- `validate_registry(registry_path=None) -> ValidationResult`:
  - `utils.load_models_config()` + `utils.load_strategies_config()` config-gatewayn át.
  - `utils.open_registry_connection()` — `reg.*` namespace.
  - Ellenőrzi: models.json model_id-k megvannak-e a `reg.models`-ben; asset_id egyezés; strategies.json kulcsok megvannak-e a `reg.strategies`-ben; strategy model_id egyezés (side szerint long/short).
  - Ha a registry táblák még üresek (nem populáltak), `DEBUG` log + skip (nem ront el).
  - Minden eltérés: `WARNING` log.
  - `ok=True` ha nincs eltérés.
- CLI: `python -m data_handling.store.registry_validator` — `OK` vagy `WARN` + részletek, exit 0/1.

**Teszt eredmény:**
- `ruff check src/data_handling/ --fix` — tiszta.
- `pyright src/data_handling/store/duckdb_store.py src/data_handling/store/registry_validator.py` — 0 error, 0 warning.
- `pytest src/data_handling/tests/store/smoke/ -v` — **46 passed** (0 failed).
- Pre-existing failures (scope-on kívül): `test_target_row_count_matches_features` (live DB adat-eltérés, nem tört el a refactor), `test_predictions_score_range` (élő DB-ben non-[0,1] prediction, pre-existing), `test_ensure_registry_creates_eight_tables`/`test_ensure_registry_is_idempotent` (t317 hozzáadta a v2 migrációt a registryhez, ezek a tesztek `[1]`-et várnak — t317 scope).

**Döntések / feltételezések:**
- `materialize_sample_table` teszt-fájlját (`test_materialize_sample.py`) teljesen töröltem — nincs értelme megtartani, a függvény megszűnt.
- `_seed_sample` helperben a `materialize_sample_table` logikáját direkten replikáltam (DuckDB register + CREATE OR REPLACE), hogy a `test_validate.py` validate-tesztjei önállóak maradjanak.
- A `strategies.json` backtest-időszakok (`start`/`end`) és küszöbök érintetlen maradtak — csak az `asset_id` és `model_id` mezők normalizálódtak (séma-konzisztencia, nem funkció-változás).
- A `registry_validator` a `registry_path` paramétert elfogadja (tesztelhetőségért), de az `utils.open_registry_connection()` mindig a kanonikus `utils.registry_path()`-t használja — jövőbeli per-path override-hoz van fenntartva.
