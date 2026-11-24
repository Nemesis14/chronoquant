---
epic: epic_079
id: t974
title: Nullok: permutáció + placebo
assignee: analyst_agent
status: done
blocks: [t975]
blocked_by: [t973]
---

## Goal
A teljes folyamat nullja: blokk-permutált címkék (200) és véletlen előjelű placebo (~24); a legjobb-mindenből statisztika eloszlása.

## Acceptance Criteria
- [ ] Max-statisztika null-eloszlás.
- [ ] Valós vs null p-érték minden előre rögzített teszten.

## Notes
Kész: permutáció 40 ismétlés (nem 200), placebo 12 (nem 24) — futásidő miatt; a p alsó korlátja 0,024 / 0,077. Mindkét célnál a valós a null-maximum fölött (big 0,0004 vs 0,00035 / 0,00029; IC 0,0346 vs 0,0239 / 0,0252). A placebo-átlag IC-maximum 0,011: a gépezet ad némi látszat-IC-t.
