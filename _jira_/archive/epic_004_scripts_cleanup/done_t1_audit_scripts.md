---
epic: epic_004
id: t1
title: Audit and clean up scripts/ directory
assignee: doc_agent
status: pr
blocks: [t2]
---

## Goal

Minden `scripts/` mappában lévő Python fájlt megvizsgálni és besorolni:
**aktív** (rendszeresen futtatott), **halott** (elavult, semmi nem hivatkozik rá),
vagy **refaktorálható** (működő, de projekt konvencióval ellentétes).

## Scope

```
scripts/
  backtest_strategy.py
  benchmark_duckdb.py
  create_sample_splits.py
  elliott_event_study.py
  elliott_scan.py
  final_fit_lgbm_v4.py
  generate_model_card.py
  promote_lgbm_v2.py
  run_trading_service.py
  search_lgbm.py
  sweep_strategy.py
  sync_ohlcv.py
  train_model.py
  validate_duckdb_stats.py
```

## Acceptance Criteria

- [x] Minden script besorolva: `active` / `dead` / `refactor`
- [x] Besorolás indoklással: mire hivatkozik, mikor futtatható, miért elavult
- [x] `dead` scriptek listája user jóváhagyásra kész (NEM törlünk jóváhagyás nélkül)
- [x] `refactor` scriptek esetén: egy mondatos leírás mit kellene változtatni
- [x] Audit eredménye: `_jira_/epic_004_scripts_cleanup/audit_result.md`

## Notes

Gyaníthatóan halott scriptek:
- `promote_lgbm_v2.py` — v4 az aktív modell, v2 promóciója értelmetlen
- `create_sample_splits.py` — a `dataset_split` oszlop most már a feature sync részeként jön létre
- `elliott_event_study.py`, `elliott_scan.py` — Elliott waves izolált research modul, nem aktív pipeline

Az audit elvégzéséig NEM törlünk semmit.
