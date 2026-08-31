---
epic: epic_065
id: t869
title: Módszertani rationale dokumentáció és döntési javaslat
assignee: methodology_agent
status: todo
blocks: []
blocked_by: [t867]
---

## Goal

A klaszter-alapú setup-felismerés módszertani hátterét, döntéseit és a t867 OOS
validáció eredményét (pozitív vagy negatív) rögzíteni a `_doc_/methodology_doc/`
zónában, kód-mentes, üzleti/módszertani nyelven.

## Scope

- `_doc_/methodology_doc/` — új dokumentum a sorozat következő számával.
- Tartalma: az ötlet, a módszertani döntések (mintavétel, normalizálás, klaszterezés,
  OOS validáció — lásd `_doc_/_plans_/plan.md`), a t867 eredmény összefoglalója,
  és egy explicit **Fázis 3 döntési javaslat**:
  - ha van OOS-stabil, elég effektív N-nel rendelkező klaszter: javaslat a
    bevezetés módjára (önálló stratégia vs. LGBM-kiegészítő feature),
  - ha nincs: a negatív eredmény dokumentálása, hogy a jövőben ne kelljen
    ugyanezt újra megvizsgálni tisztázatlan okból.

## Acceptance Criteria

- [ ] Dokumentum létrehozva `_doc_/methodology_doc/`-ban, a zóna konvenciói szerint
      (kód-mentes, rationale-fókuszú)
- [ ] Hivatkozik a `_doc_/_plans_/plan.md`-re és a t867 validációs eredményre
- [ ] Explicit döntési javaslat a Fázis 3 lépésre (bevezetés / lezárás / módosítás)
- [ ] Ha negatív az eredmény, ez legalább annyira részletesen dokumentált, mint egy
      pozitív eredmény lenne

## Notes
