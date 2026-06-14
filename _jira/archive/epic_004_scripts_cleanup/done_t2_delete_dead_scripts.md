---
epic: epic_004
id: t2
title: Delete confirmed dead scripts after user approval
assignee: doc_agent
status: pr
blocked_by: [t1]
---

## Goal

A t1 audit alapján `dead` besorolást kapott scripteket törölni, miután
a user jóváhagyta a listát.

## Scope

`scripts/` — csak a t1 auditban `dead`-nek minősített fájlok.

## Acceptance Criteria

- [x] User explicit jóváhagyása a törlési listára megvan (t1 audit_result.md alapján)
- [x] Törölt fájlok git-ből is eltávolítva
- [x] `refactor` besorolású scriptek érintetlenek

## Notes

Ez a task NEM indítható el t1 befejezése és user jóváhagyása nélkül.

Implementáció (2026-06-14): user jóváhagyta. Törölve:
- `scripts/modeling/create_sample_splits.py`
- `scripts/research/elliott_event_study.py`
- `scripts/research/elliott_scan.py`

Fennmaradó aktív scriptek: sync_ohlcv, backtest_strategy, generate_model_card,
sweep_strategy, search_lgbm, train_model, benchmark_duckdb, validate_duckdb_stats,
run_trading_service.
