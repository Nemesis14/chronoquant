---
epic: epic_018
id: t1
title: Sampling kód átvizsgálása az új spec alapján
assignee: modeling_agent
status: done
blocks: [t5]
---

## Goal

Az átrendezés és architektúra döntések után a sampling kód (`src/modeling/sampling/`) megfelelőségének ellenőrzése az új specifikáció alapján.

## Scope

- `src/modeling/sampling/config.py`
- `src/modeling/sampling/yearly_sampler.py`
- `src/modeling/sampling/create_sample.py`
- `src/modeling/sampling/artifacts.py`
- `src/modeling/sampling/audit.py`
- `src/modeling/tests/sampling/`

## Acceptance Criteria

- [ ] `test_months` paraméter eltávolítva a `YearlySamplingConfig`-ból (default volt 1, de sosem használtuk)
- [ ] `segment` értékek: csak `train / valid / purge` — nincs `test`
- [ ] `write_yearly_artifacts` `sample_train_valid.parquet`-et ír (már kész)
- [ ] `load_yearly_sample` `sample_train_valid.parquet`-et olvas (már kész)
- [ ] `materialize_sample_table` hívás eltávolítva `create_sample.py`-ból (DuckDB materialization megszűnt)
- [ ] Összes teszt átmegy: `uv run pytest src/modeling/tests/sampling/ -v`
- [ ] `ruff check` + `pyright` tiszta

## Notes

Spec forrása: `_doc_/0000_project_overview.md` — "Yearly sample model" szekció.

A meglévő 5 sample parquet (`solusdt_fw60_yearly_2021`–`2025`) már `sample_train_valid.parquet` névre van átnevezve.
