---
epic: epic_060
id: t813
title: Long/short osztályok egymáshoz kalibrálása
assignee: modeling_agent
status: todo
blocks: [t814]
blocked_by: [t812]
---

## Goal

Az „a modell válassza az irányt" ág azért bukott el, mert a `P(+1)`
szisztematikusan magasabb a `P(−1)`-nél — eső piacon is 108 long jel jött
5 short mellett a p99-en. A két osztály **nincs egymáshoz kalibrálva**.

## A megoldás — két változat, mindkettő mérve

1. **Irányonként külön kvantilis** (ma ez fut): mindkét irány a saját
   eloszlásának p95-jét kapja → a jelszám kiegyenlített, de a két jel
   nem összemérhető, ha egyszerre jön.
2. **Közös kalibráció**: `P(+1) − P(−1)` előjeles score, egyetlen eloszlás,
   szimmetrikus küszöb. Egy jel, nincs ütközés.

## Acceptance Criteria

- [ ] Mindkét változat lemérve azonos modellen
- [ ] Kimondva, melyik ad több nettó bps/kötést
- [ ] Az ütközések száma (long ÉS short jel ugyanabban a percben) riportálva

## Notes
[orchestrator] NEM FUTOTT — 2026-08-26

A t812 utan a long-elfogultsag mar nem a fő szuk keresztmetszet: a
vol-skalazott cimken a SHORT oldal **jobb** lett a LONG-nal (49,7% vs 47,3%),
tehat a `P(+1) > P(−1)` aszimmetria nem az, ami a rendszert visszatartja.

A varhato hozadek (~1 bps/kotes) nem indokolja a futasidőt, amig az
`epic_062` nyitott es ott mar pozitiv szam van. **Fuggesztve, nem torolve** —
ha a direkcios ag ujraindul, ez az elso lepes.
