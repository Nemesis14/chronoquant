---
epic: epic_6
id: t3
title: Create sampling/audit.py
assignee: modeling_agent
status: pr
blocks: [t5]
---

## Goal
Implementálni az `audit_feature_table()` függvényt, amely DuckDB native SQL-lel
meghatározza a biztonságos adattartomány határait és minőségi mutatókat ad vissza.
Ez a sampling modul kulcs-újdonsága: safe `data_start` és `data_end` a null-ellenőrzések alapján.

## Scope
- `src/modeling/quantitative/sampling/audit.py` (új)
- `src/database/store/duckdb_query.py` (olvasás, `dataset_columns` újrahasználva)

## Acceptance Criteria
- [ ] `audit_feature_table(db_path: str, target_col: str) -> dict` publikus függvény
- [ ] `data_start_safe`: első `open_time` ahol MINDEN `feat_*` oszlop NOT NULL
  - `dataset_columns()` adja a `feat_*` listát, majd DuckDB native SQL WHERE-klausula
- [ ] `data_end_safe`: utolsó `open_time` ahol `target_col` IS NOT NULL (a `target` táblában)
- [ ] Visszatérési dict tartalmaz: `data_start_safe`, `data_end_safe`, `row_count`, `unique_timestamps`, `duplicate_count`, `target_null_count`, `feature_null_summary: {col: null_rate}`, `gap_count`, `gap_minutes_total`
- [ ] Minden query: DuckDB native SQL — **nincs Polars, nincs pandas**
- [ ] Gap detekció: egy aggregált SQL-lel (LAG window function, ahol `open_time - lag > 1 perc`)
- [ ] Coding standard: modul docstring, Google-style docstring, `# %%` markerek
- [ ] `uv run pyright src/modeling/quantitative/sampling/audit.py` hibátlan

## Notes
A `feat_*` WHERE klausula dinamikusan generálódik a `dataset_columns()` visszatérési
listájából. A feature null summary-hoz érdemes egyetlen DuckDB aggregáció: 
`AVG(CASE WHEN feat_x IS NULL THEN 1.0 ELSE 0.0 END)` minden oszlopra.
A `target` és `feat_ohlcv_quant` külön táblák — két külön query.
