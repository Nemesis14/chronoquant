---
epic: epic_028
id: t129
title: Validator session — trading module cleanup
assignee: validator_agent
status: todo
blocks: []
blocked_by: [t125, t126, t127, t128]
---

## Goal

Validálni az epic_028 összes pr_ taskját: ruff + pyright + pytest, majd done_ státuszra emelni.

## Scope

- `src/trading/`
- `src/ui/trading_runner.py`
- `src/ui/sync.py` (ha t125 érinti)

## Acceptance Criteria

- [ ] `uv run ruff check src/trading/ --fix` — 0 hiba
- [ ] `uv run ruff check src/ui/trading_runner.py src/ui/sync.py --fix` — 0 hiba
- [ ] `uv run pyright src/trading/` — 0 hiba
- [ ] `uv run pyright src/ui/trading_runner.py` — 0 hiba
- [ ] `uv run pytest src/trading/tests/ -v` — minden teszt zöld
- [ ] Minden pr_t125–pr_t128 → done_ átnevezve

## Notes

Ha bármely lépés fail-el: a releváns pr_ ticket visszakerül todo_-ba, Notes szekcióba a hiba leírva.

[validator] 2026-06-20
Passed gates:
- `uv run ruff check src/trading/ --fix`
- `uv run ruff check src/ui/trading_runner.py src/ui/sync.py --fix`
- `uv run pyright src/trading/`
- `uv run pyright src/ui/trading_runner.py`
- `uv run pytest src/trading/tests/ -v`

CLI smoke:
- `uv run python src/trading/01_run_service.py --mode dry_run`
- Service startup succeeded after setting `config/trading.json.strategy_session_id` to the existing strategy artifact session.
- Runtime warning remained about missing active champion model during prediction sync, but no startup exception occurred and the service shut down cleanly.
