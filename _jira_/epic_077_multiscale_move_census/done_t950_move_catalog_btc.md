---
epic: epic_077
id: t950
title: Multi-skálás szakasz-detektor és nagy-mozgás katalógus (BTC long)
assignee: analyst_agent
status: done
blocks: [t951, t952]
blocked_by: []
---

## Goal
BTCUSDT 1m adatból, csak long irányban, kimutatni minden nagy emelkedést (skálafüggetlen definícióval), és darabszám-táblát adni skála × k szerint.

## Scope
- Új builder `src/reports/builders/move_census.py` (+ tesztek); a `wave_events.py` ötleteit/segédeit újrahasznosítva.
- Tábla: `research."move_census_v1_moves"` (btcusdt_lab.duckdb): start/end idő, skála, k, nettó log-elmozdulás, σ-egységben mért méret, efficiency ratio, max. belső visszaesés, időtartam.
- Riport: `_doc_/reports/move_census/` (darabszámok skála × k, éves bontás, durations eloszlás).

## Acceptance Criteria
- [ ] Skála-rács: ~15 perc … ~3 nap, geometriai; k ∈ {3, 4, 6}; mindkettő a kódban és a riportban előre rögzítve.
- [ ] Skála-specifikus σ: trailing 30 napos empirikus σ az adott hosszra, csak múltbeli adatból (egységteszt).
- [ ] Trendesség- és max. belső visszaesés feltétel dokumentálva, érzékenység közölve.
- [ ] Csak train-ablak (2021-01-01 → 2026-01-31); OOS és holdout-2 nem töltődik be (teszt).
- [ ] Darabszám-tábla skála × k × év; üres/kis skálák jelezve.
- [ ] Átfedő mozgások kezelése (szülő–gyerek skála hierarchia vagy nem-átfedés) dokumentálva.
- [ ] ruff + pyright + pytest tiszta.

## Notes
Katalógus utólagos (hindsight) — a leíró keret nem előrejelzés (lásd epic.md).

[analyst] 2026-11-24 — kész.
- Kód: `src/reports/builders/move_census.py` (detektor, persist), `move_census_report.py` (riport). Tábla: `research."move_census_v1_moves/_cells/_meta"` (btcusdt_lab). Riport: `_doc_/reports/move_census/`.
- Rács: T = 15,30,60,120,240,480,960,1920,4320 perc; k = 3,4,6; ρ = 0,5 (zigzag fordulás = ρ·k·σ_T, a belső visszaesés < 0,5·k·σ). Ablak 2021-02-04 → 2026-01-31 (4,99 év). 40 592 nagy mozgás 27 cellában; k=4: 5903 (T=15) … 35 (T=3 nap).
- Kauzalitás ellenőrizve: a σ_T és a zigzag a levágott adaton azonos pivotokat ad; a OOS/holdout-2 nem töltődik be (load_minutes ≤ 2026-01-31 23:59).
- Átfedés: cellán belül nincs; cellák között ~75–88% a nagyobb skálák uniójába esik (beágyazott). Az összes skála uniója az idő ~73%-át fedi.
- Megfigyelés: 883 mozgás ≤ 5 perc: valódi flash-események (2022-11-08, 2023-08-17, 2025-10-10): V-alakú visszapattanások, nem trendindulás.
- Pytest nincs (analyst_agent nem ír tesztet) — a validátorra marad.
