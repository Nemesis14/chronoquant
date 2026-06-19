---
epic: epic_021
id: t4
title: Validation
assignee: validator_agent
status: todo
blocks: []
blocked_by: [t3]
---

## Goal

Az epic t1–t3 taskjai után ruff + pyright + pytest futtatása a modeling modulon.

## Scope

- `src/modeling/training/`
- `src/modeling/pipeline.py`
- `src/modeling/03_fit_model.py`

## Acceptance Criteria

- [ ] `uv run ruff check src/modeling/ --fix` — clean
- [ ] `uv run pyright src/modeling/` — 0 error
- [ ] `uv run pytest src/modeling/ -v` — minden test zöld
- [ ] `pr_` státuszra mozgatja t1, t2, t3 ticketjeit

## Notes

Ha pyright hibát talál a típus-annotációknál (pl. nullable parquet betöltésnél),
javítsa a forrás fájlban — ne ignorálja.
