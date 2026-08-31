---
epic: epic_063
id: t841
title: Független univerzum — a 31–60. legforgalmasabb pár
assignee: analyst_agent
status: todo
blocks: [t844]
blocked_by: []
---

## Goal

A legszigorúbb egyetlen teszt: **ugyanaz a kód, ugyanazok a paraméterek,
teljesen más 30 pár.**

## ⚠️ Semmit nem hangolunk

Horizont 72h, decilis 20%, top-liquid szűrő nélkül (az új univerzum eleve
kevésbé likvid — ez a szigorúbb eset). A modell paraméterei változatlanok.
Ha bármit hozzáigazítunk az új univerzumhoz, a teszt értelmét veszti.

## Scope

- `build_universe.py` — `--rank-offset` kapcsoló (a 31–60. helyezett)
- `database/_universe/universe.duckdb` — külön tábla vagy külön fájl

⚠️ Az új univerzum **nem írhatja felül** a meglévőt — a `t833` eredménye
reprodukálható kell maradjon.

## Acceptance Criteria

- [ ] 30 új pár, ugyanazzal a survivorship-mentes recepttel
- [ ] 72h / 20% decilis, változatlan modell-paraméterek
- [ ] Nettó bps/pár, rezsim-bontással, havi pozitív arány
- [ ] Az epic.md döntési szabálya alkalmazva

## Notes
