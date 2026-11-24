---
epic: epic_079
id: t973
title: Futtatás: modellek x célok x szegmensek
assignee: analyst_agent
status: done
blocks: [t974]
blocked_by: [t972]
---

## Goal
Az előre rögzített modell × cél × szegmens rács futtatása a valós adaton.

## Acceptance Criteria
- [ ] Eredmény-tábla minden cellára.
- [ ] Szegmensek: összes, chain, trend, tiszta rezsim.
- [ ] OOS érintetlen.

## Notes
Kész: 5 cél × M1–M4, 6 külső blokk, 7 skála, 6 szegmens (tábla: `cand_v2_oof/_metrics/_slices`). Nagy mozgás: M4 nyereség +0,00043 (CI alsó +0,00006), AUC 0,537 vs 0,531; +1R: nincs jel; hozam IC 0,027–0,039. Szegmens-specifikus külön tanítás nem készült (a szeletek csak kiértékelési bontások).
