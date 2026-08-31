---
epic: epic_064
id: t857
title: Verdikt
assignee: analyst_agent
status: todo
blocks: []
blocked_by: [t856]
---

## Goal

Az `epic.md`-ben előre rögzített szabály alkalmazása, az eredmények
ismerete nélkül megfogalmazva.

| Kimenet | Verdikt |
|---|---|
| Mindkét likviditási sávban ÉS mindkét rezsimben fedezeti pont fölött | **GO** |
| Csak az egyikben | részleges — a merítés nem elég |
| Sehol | **A merítés nem oldja meg. A direkciós ág véglegesen zárul.** |

## Acceptance Criteria

- [ ] `_doc_/research/multi_coin_direct_gate.md` riport
- [ ] A döntési szabály alkalmazva, a verdikt kimondva
- [ ] Összevetés: a direkciós ág egy coinon **−2,78 bps/kötés** volt
  (`atr_barrier_gate.md`), a piac-semleges könyv **+22,5 bps/pár**, de
  Sharpe 0,51 / t ≈ 0,7 (`cross_sectional_probe.md`)
- [ ] Ha negatív: ugyanolyan egyértelműen, mint egy GO

## Notes
