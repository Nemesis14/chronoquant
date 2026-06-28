# Epic 037: Code Cleanup and Refactor

## Goal

Első prototípus audit alapján: halott kód eltávolítása, redundanciák megszüntetése,
architektúrális problémák kezelése és coding standards betartatása. A repo
funkcionalitása nem változik — tisztán strukturális és minőségi javítások.

## Scope

- `src/data_handling/store/` — `_tbl_exists` dedup, `query_range` merge, DDL removal
- `src/ui/` — race condition, dead functions, silent exceptions, dedup
- `src/trading/live/` — `_dash_log` fix, `_next_open_time` dedup
- `src/modeling/` + `src/strategy/` — snapshot/artifact helper dedup, pyright fixes
- `src/utils.py` — dead function removal, ruff fixes
- `src/trading/live/exchange.py` — `_SOL_QTY_STEP` config-driven

## Tasks

- t250: Data handling deduplication (database_agent) [HIGH]
- t251: UI high-priority fixes — race condition, _dash_log, silent exceptions (ui_agent) [HIGH]
- t252: UI dead code removal + dedup (ui_agent) [MEDIUM]
- t253: Modeling/strategy deduplication + pyright fixes (modeling_agent) [MEDIUM]
- t254: Utils cleanup + ruff/pyright fixes (code_doc_agent) [LOW]
- t255: Full validation — ruff + pyright + pytest (validator_agent)

## Végrehajtási sorrend

t250, t251, t254 párhuzamosan →
t252 (blocked_by t251), t253 (blocked_by t250) →
t255 (blocked_by t250–t254)

## Key Decisions

- Funkcionális változás nélkül: minden megmaradó kód viselkedése azonos marad.
- `_resolve_path`/`_repo_root` rename (20+ caller) halasztva — külön epicbe kerülhet,
  ha az összes agent egyszerre dolgozna rajta.
- `_tbl_exists` canonical helye: `src/data_handling/store/duckdb_query.py`.
  A `build_table.py` (strategy) innen importálja.
