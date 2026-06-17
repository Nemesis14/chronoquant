---
epic: epic_012
id: t4
title: Validátor — epic_012 tesztelés és lezárás
assignee: validator_agent
status: done
blocked_by: [t1, t2, t3]
---

## Goal

Minden epic_012 task elvégzése után: teljes database test suite lefut, ruff és pyright clean.

## Scope

- `src/database/tests/` teljes könyvtár

## Acceptance Criteria

- [ ] `ruff check src/database/tests/ --fix` — 0 hiba
- [ ] `uv run pyright src/database/tests/` — 0 új error (pre-existing hibák száma nem nő)
- [ ] `uv run pytest src/database/tests/ -v` — **0 FAILED** (jelenleg 16 FAILED, mind Polars-migráció miatt)
- [ ] `pr_t1`, `pr_t2`, `pr_t3` → `done_t1`, `done_t2`, `done_t3`

## Notes
