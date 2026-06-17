---
epic: epic_6
id: t4
title: Create sampling/artifacts.py
assignee: modeling_agent
status: pr
blocks: [t5]
---

## Goal
Refaktorálni a meglévő `sampling.py` IO logikáját új `artifacts.py`-ba. Minden
fájl írás/olvasás/validáció egy helyen. Az új `write_sample_artifacts()` mindig
három fájlt ír: `metadata.json`, `folds.json`, `audit.json`.

## Scope
- `src/modeling/quantitative/sampling/artifacts.py` (új)
- Meglévő logika forrása: `src/modeling/quantitative/sampling.py` (nem módosítjuk, t6-ban töröljük)

## Acceptance Criteria
- [ ] `write_sample_artifacts(sample_dir: Path, metadata: dict, folds: dict, audit: dict) -> None`
  - Mindig ír 3 fájlt: `metadata.json`, `folds.json`, `audit.json`
  - `sample_dir` létrehozása ha nem létezik
- [ ] `load_sample_definition(sample_dir: str | Path) -> dict` — meglévő logika refaktorálva
- [ ] `validate_sample_definition(sample: dict) -> None` — meglévő logika refaktorálva
- [ ] `metadata.json` tartalmaz `generated_at` (UTC ISO string) és `source` mezőt:
  ```json
  "source": {
    "db_relative_path": "database/solusdt/solusdt.duckdb",
    "feature_table": "feat_ohlcv_quant",
    "target_table": "target"
  }
  ```
  **Relatív path** — nem abszolút
- [ ] Coding standard: modul docstring, Google-style docstrings, `# %%` markerek, alignment
- [ ] Nincs pandas import — csak stdlib (`json`, `pathlib`, `datetime`)
- [ ] `uv run pyright src/modeling/quantitative/sampling/artifacts.py` hibátlan

## Notes
A `load_sample_definition()` és `validate_sample_definition()` jelenleg a `sampling.py`-ban
van — ezeket importálják a `lgbm_search.py` és `lightgbm_model.py`. Az `__init__.py`
(t5) gondoskodik a visszafelé kompatibilis re-exportálásról.
