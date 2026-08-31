---
epic: epic_060
id: t814
title: Verdikt — megoldja-e a skálázott korlát a rezsim-problémát
assignee: analyst_agent
status: todo
blocks: []
blocked_by: [t813]
---

## Goal

Az epic.md-ben előre rögzített döntési szabály alkalmazása, az eredmény
ismerete nélkül megfogalmazva.

## Acceptance Criteria

- [ ] `_doc_/research/atr_barrier_gate.md` riport
- [ ] A döntési szabály alkalmazva, a verdikt kimondva
- [ ] Ha negatív: ugyanolyan egyértelműen, mint egy GO

## Notes
[orchestrator] Elvegezve — 2026-08-26

Riport: `_doc_/research/atr_barrier_gate.md`

**VERDIKT: RESZLEGES** — az elore rogzitett szabaly szerint nem GO.

| Feltetel | Elvart | Mert |
|---|---|---|
| `talalat − fedezeti` > 0 mind a 4 cellaban | igen | **nem** (legjobb −0,23) |
| Osszesitett netto bps/kotes > 0 | — | **nem** (−2,78) |
| Osztalyeloszlas 10pp-en belul (belepesi feltetel) | igen | 13,1pp (a padlo miatt) |

## De a hipotezis igazolodott

| | fix ±100 bps | vol-skalazott |
|---|---|---|
| bps/kotes | −5,26 | **−3,31** |
| cellak szorasa | 14,9 bps | **5,9 bps** |
| legrosszabb cella | −12,51 | −6,14 |

**A rezsim-fugges 2,5-szeresere csokkent.** Ez volt a cel, es teljesult.
A rendszer megsem nyereseges — de a maradek hiany mar nem rezsim-eredetu,
hanem egyszeruen keves a jel.

## Dontes

- **A `tb_label_240_vol` az uj alapertelmezett cimke.** A fix ±100 bps legacy.
- t813 (long/short kozos kalibracio) **nem futott** — a t812 utan a varhato
  hozadeka (~1 bps) nem indokolja a futasidot, amig az epic_062 nyitott.
- Az ag **nem zarul le**, de nem is prioritas: az `epic_062` ugyanazt a
  bizonyitott kepesseget (rangsorolas) hasznalja, es ott mar pozitiv szam van.
