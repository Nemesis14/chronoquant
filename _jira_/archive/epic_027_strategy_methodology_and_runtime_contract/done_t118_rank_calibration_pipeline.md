---
epic: epic_027
id: t118
title: Implementald a rank calibration pipeline-t
assignee: modeling_agent
status: done
blocks: [t119]
blocked_by: [t116]
---

## Goal

A strategy table-re olyan calibration réteget építeni, amely a user által megadott
egyetlen strategy időszakon a raw long/short score-okból percentile/decilis mezőket
és expectancy lookupokat képez.

## Scope

- `src/strategy/strategy/build_table.py`
- `src/strategy/strategy/calibrate.py`
- strategy session artifactok

## Acceptance Criteria

- [x] A strategy table tartalmazza a runtime-hoz szükséges rank-alapú mezőket
- [x] A calibration nem csak abszolút score-szintet, hanem relatív rangot is ment
- [x] A long és short irány külön calibrationt kap
- [x] A calibration ugyanazon strategy session időablakon fut, amelyre a search is épül
- [x] Az output később a trading runtime-ban újrafittelés nélkül alkalmazható

## Notes

Az isotonic maradhat opcionális másodlagos rétegként, de a primary signal contract rank-first legyen.

2026-06-20 — Implementálva. `fit_calibration` szignatúra átírva (`calib_start/end` → `start/end`).
`_build_rank_lookup` helper hozzáadva: rendezi a calib periódus score-jait, kiszámítja a percentile rankot
(`arange(1,n+1)/n`), decilis bucket-et (`min(ceil(pct*10),10)`), és denormalizált bucket statokat (mean_mfe,
hit_rate). A teljes strategy_table-re np.interp alapján terjeszti ki a rank mezőket. Isotonic fitting megmaradt
secondary rétegként. Artifacts: rank_lookup_long.parquet, rank_lookup_short.parquet, isotonic_long.pkl,
isotonic_short.pkl. CLI (`01_calibrate_scores.py`) és smoke tesztek frissítve. ruff clean, pyright 0 error,
pytest 1 passed.

### Validator (t121) — 2026-06-20

- ruff check src/strategy/: 1 auto-fixed, 0 remaining
- pyright src/strategy/strategy/: 0 errors, 0 warnings
- pytest src/strategy/tests/: 8/8 passed
- status: done
