---
epic: epic_027
id: t116
title: Strategy methodology es 6xxx doc reset
assignee: methodology_agent
status: pr
blocks: [t117, t118, t119, t120]
blocked_by: []
---

## Goal

Szétválasztani a strategy és trading dokumentációs felelősségeket, és létrehozni az új
`_doc_/6000_strategy.md` fejezetet, amely kizárólag a strategy metodológiát írja le.

## Scope

- Törlendő: `_doc_/6000_trading.md`
- Törlendő: `_doc_/6010_strategy_calibration.md`
- Törlendő: `_doc_/6100_calibration.md`
- Törlendő: `_doc_/6200_live_service.md`
- Új fájl: `_doc_/6000_strategy.md`
- Frissítés: `_doc_/README.md`

## Acceptance Criteria

- [x] A 6xxx blokkban csak egy metodológiai fájl maradjon: `_doc_/6000_strategy.md`
- [x] Az új fájl tisztán a strategy domainről szóljon, ne a live trading implementációról
- [x] A dokumentum rögzítse a rank-first signal methodologyt és az artifact/runtime contract alapjait
- [x] A dokumentum írja le, hogyan illeszkedik az új strategy logic a meglévő `src/strategy/` artifact irányhoz

## Notes

[methodology_agent] Elvégezve - 2026-06-20

- A 6xxx dokumentációs blokk át lett rendezve a kért számozási logika szerint.
- Az új `6000_strategy.md` a strategy domain metodológiát rögzíti.
- A dokumentumban a javasolt irány: rank/decilis-alapú signal building, opcionális isotonic kiegészítő réteggel.
- A persistence irány megtartja a meglévő `strategy_table.parquet` + `strategy_artifact.json` logikát, de kiegészíti lookup-contracttal, hogy a trading runtime ugyanazt a signal-transzformációt tudja alkalmazni.

[validator] done — 2026-06-20. Doc criteria verified: 6000_strategy.md exists with rank-first content; old 6xxx files absent; README updated.
