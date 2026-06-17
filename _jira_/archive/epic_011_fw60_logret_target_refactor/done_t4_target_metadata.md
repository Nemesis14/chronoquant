---
epic: epic_011
id: t4
title: Új target metadata — fw60 outcome definitions
assignee: database_agent
status: todo
blocked_by: [t3]
blocks: [t5]
---

## Goal

A target sync futás után a metadata fájl dokumentálja az összes fw60 outcome oszlop definícióját.

## Scope

```
src/database/sync_tables/sync_targets.py   (metadata írás logika)
database/solusdt/solusdt.json              (vagy az asset metadata fájl)
```

## Javasolt metadata struktúra

```json
{
  "target_outcomes": {
    "fw60": {
      "horizon": 60,
      "window": "t+1..t+60",
      "columns": {
        "close": "close[t] — reference bar close",
        "fw60_close": "close[t+60] — raw forward close",
        "fw60_max": "max(close[t+1:t+60]) — raw max price",
        "fw60_min": "min(close[t+1:t+60]) — raw min price",
        "fw60_close_ret": "close[t+60] / close[t] - 1",
        "fw60_close_logret": "log(close[t+60] / close[t])",
        "fw60_max_ratio": "max(close[t+1:t+60]) / close[t]",
        "fw60_min_ratio": "min(close[t+1:t+60]) / close[t]",
        "long_mfe_fw60": "log(max(close[t+1:t+60]) / close[t]) — LONG TARGET",
        "short_mfe_fw60": "log(min(close[t+1:t+60]) / close[t]) — SHORT TARGET"
      },
      "null_tail_rows": 60,
      "computed_from": "<timestamp>",
      "computed_to": "<timestamp>"
    }
  }
}
```

## Acceptance Criteria

- [ ] Metadata fájl tartalmazza az összes fw60 outcome oszlop definícióját
- [ ] `computed_from` / `computed_to` frissül minden sync futásnál
- [ ] `null_tail_rows: 60` dokumentálva
- [ ] Nincs régi `derived_binary_targets` szekció

## Notes
