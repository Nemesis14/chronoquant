---
epic: epic_014
id: t4
title: Validáció — ruff + pyright + pytest (új unit tesztek a sampling logikára)
assignee: validator_agent
status: pr
story_points: 3
blocks: []
blocked_by: [t3]
---

## Goal

A t2 és t3 által létrehozott kód és artifaktok validálása: statikus analízis (ruff, pyright)
és automatizált tesztek (pytest). Új unit tesztek írása a yearly_sampler pure function-ökhöz.

## Scope

Érintett modulok:
- `src/modeling/quantitative/sampling/`
- `src/modeling/quantitative/00_create_sample.py`

## Acceptance Criteria

### Statikus analízis
- [x] `ruff check src/modeling/quantitative/sampling/ --fix` — clean (0 error)
- [x] `uv run pyright src/modeling/quantitative/sampling/` — clean (0 error)
- [x] `ruff check src/modeling/quantitative/00_create_sample.py` — clean

### Pytest — új tesztek
Hely: `src/modeling/quantitative/tests/sampling/smoke/` (meglévő struktúrához igazítva)

- [x] `test_hourly_selection.py`:
  - Pontosan 1 sor / óra a kimenetben
  - Azonos seed → azonos kimenet (reprodukálhatóság)
  - Különböző seed → különböző kimenet
  - Minden kiválasztott sor az eredeti adatban szerepel

- [x] `test_monthly_validation_weeks.py`:
  - Pontosan 12 validation week / év
  - Minden validation week más hónapból való
  - Minden validation week pontosan 7 nap
  - Fixed seed → reprodukálható

- [x] `test_segment_assignment.py`:
  - Valid és train set-ben nincs overlap (open_time szintjén)
  - Purge sorok nem szerepelnek sem train, sem valid-ben
  - Purge ablak korrekt: ±240 perc a valid week határain
  - Segment értékek csak: {"train", "valid", "purge"}

### Artifakt integritás
- [x] `sample.parquet` olvasható Polars-szal minden évben
- [x] `metadata.json` JSON-valid és tartalmaz kötelező kulcsokat (year, seed, selected_valid_weeks, row_counts)
- [x] `audit.json` JSON-valid

## Notes

Tesztek synthetic mini-adatokon fussanak (ne igényeljenek DB kapcsolatot). A pure
function-ök (hourly selection, segment assignment) izoláltan tesztelhetők egy kis
Polars DataFrame-mel.

Ha a `src/modeling/quantitative/sampling/tests/` mappa nem létezik, hozza létre a validator.

[validator] 2026-06-17 — Minden check teljesült.
- ruff fix: 1 I001 import-rendezési hiba volt artifacts.py-ban — fixálva.
- pyright: 0 error, 0 warning.
- pytest: 33/33 passed (19 új + 14 meglévő).
  Tesztek helye: src/modeling/quantitative/tests/sampling/smoke/ (meglévő struktúrához igazítva, nem új almappa).
- Artifakt integritás: mind az 5 év (2021–2025) átment — row count, 12 valid week, required keys, parquet olvashatóság.
- Polars API: frame_equal() helyett equals() — newer Polars-hoz javítva.
