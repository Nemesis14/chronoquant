---
epic: epic_056
id: t501
title: Short irány — kontroll és kezelés modell
assignee: modeling_agent
status: todo
blocks: [t503]
blocked_by: []
---

## Goal

Megismételni az `epic_054` kísérletét short irányban, ugyanazon az ablakon.

## Konfiguráció

```
target       : short_mfe_fw60
train        : 2023-01-01 -> 2026-01-31
valid        : 2026-02-01 -> 2026-07-31
snapshotok   : a MEGLEVO ketto (948cc228 = kontroll, de542d6b = kezeles)
model_id-k   : lgbm_solusdt_s_fw60_ctrl_2301_2607
               lgbm_solusdt_s_fw60_alt_2301_2607
```

A snapshotok újrahasználhatók — mindkét target oszlop benne van.

## ⚠️ Short-specifikus

A `short_mfe_fw60` **negatív** (a profitábilis short = negatív logreturn), és
a rangsor **invertált**: alacsony `score_pct_short` = legjobb short.
A strategy sessionben az entry feltétel `(1 - score_pct_short) >= cutoff`.

Ha a szám gyanúsan jó vagy gyanúsan rossz, az első ellenőrzendő az előjel.

## Acceptance Criteria

- [ ] Mindkét modell végigfutott a teljes pipeline-on
- [ ] Strategy session mindkettőre, `--directions short`
- [ ] Az `epic_054` 8 bps-es költség-tudatos összevetése megismételve
- [ ] Explicit válasz: shorton is megvan a kezelés előnye?

## Notes
