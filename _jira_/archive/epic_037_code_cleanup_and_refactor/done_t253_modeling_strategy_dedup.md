---
epic: epic_037
id: t253
title: Modeling/strategy deduplication + pyright fixes
assignee: modeling_agent
status: done
blocks: []
blocked_by: [t250]
---

## Goal

Megszüntetni a modeling és strategy modulokban levő helper-duplikátumokat,
és javítani a 36 pyright hibát a `01_rebuild_long_d10_strategy.py`-ban.
Blocked_by t250, mert a canonical `_tbl_exists` utility ott jön létre.

## Scope

- `src/modeling/predict.py`
- `src/modeling/sampling/create_sample.py`
- `src/strategy/strategy/build_table.py`
- `src/strategy/strategy/artifacts.py`
- `src/strategy/strategy/calibrate.py`
- `src/strategy/01_rebuild_long_d10_strategy.py`
- `src/modeling/search/lgbm_search.py`

## Acceptance Criteria

### 1. `_snapshot_exists` dedup

- [ ] `predict.py:170` és `create_sample.py:295` közül az egyik törlésre kerül
- [ ] Javasolt hely: `src/data_handling/store/duckdb_query.py` (ha cross-module hívás elfogadható) VAGY `src/utils.py` snapshot-helper szekció
- [ ] Mindkét korábbi definiáló fájl a shared verziót importálja

### 2. `_resolve_snapshot_id` dedup

- [ ] `predict.py:157` és `build_table.py:56` — azonos logika egységesítve
- [ ] Javasolt: `src/utils.py` snapshot helper szekciójába kerül, `predict.py` és `build_table.py` importálják

### 3. `_artifact_dir` dedup a strategy-ben

- [ ] `calibrate.py:31` saját `_artifact_dir` definíciója törölve
- [ ] `calibrate.py` importálja `artifacts.py`-ból (ami már exportálja)
- [ ] `01_rebuild_long_d10_strategy.py:35` saját `_artifact_dir`-je szintén `artifacts.py`-ból importálva
  (vagy ha a script izolált marad, legalább a többi reimplementáció — `_repo_root`, direkt JSON olvasás — szüntessük meg)

### 4. `_tbl_exists` a `build_table.py`-ban

- [ ] `build_table.py:74` saját `_table_exists` implementációja törölve
- [ ] Importálva: `from src.data_handling.store.duckdb_query import _tbl_exists` (vagy a t250 után kialakult canonical névvel)

### 5. Pyright fixes — `01_rebuild_long_d10_strategy.py`

- [ ] Mind a 36 pyright hiba javítva (18 tényleges hely × 2 overload)
- [ ] Pattern: `Decimal(...)` kapja `float(df.loc[...].item())` vagy `str(float(...))` — nem raw Series
- [ ] Futtatás után: `uv run pyright src/strategy/01_rebuild_long_d10_strategy.py` = 0 hiba

### 6. `n_jobs` config-driven

- [ ] `lgbm_search.py:46` — `"n_jobs": 4` helyett `"n_jobs": os.cpu_count() or 4`
- [ ] `import os` hozzáadva ha még nincs

## Notes

A `_snapshot_exists` és `_resolve_snapshot_id` jó jelölt a `utils.py`-ba,
mert snapshot ID kezelés cross-cutting concern (modeling + strategy mindkettő használja).
Ha a team úgy dönt hogy `duckdb_query.py`-ba kerüljön (adatkezelési réteg), az is elfogadható.
A döntést Notes szekcióban dokumentáljuk.

A `01_rebuild_long_d10_strategy.py` 36 pyright hibája mind ugyanazon root cause-ból ered:
pd.Series/df.loc[] eredmény közvetlenül Decimal()-ba adva scalar helyett.

[modeling_agent] Implementálva 2026-06-23

Döntések:
- `_snapshot_exists` és `_resolve_snapshot_id` → `src/utils.py` snapshot helpers szekcióba kerültek (cross-cutting concern).
- `_tbl_exists` in `duckdb_query.py` bővítve opcionális `schema` paraméterrel, hogy a `build_table.py` schema-aware hívásait kiszolgálja.
- `datasets.py` (DEPRECATED modul): t250 `query_range()` return type broadening miatt 8 pre-existing pyright hiba javítva `cast(pd.DataFrame, ...)` technikával.
- Pyright fixes in `01_rebuild_long_d10_strategy.py`: `.item()` call a Series aggregation skalárjaihoz, `# type: ignore[arg-type]` a groupby bucket_id `Hashable` típushoz.
- `ruff check --fix` + `pyright src/modeling/ src/strategy/` = 0 hiba.
