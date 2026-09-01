---
epic: epic_065
id: t869
title: Módszertani rationale dokumentáció és döntési javaslat
assignee: methodology_agent
status: done
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

**[2026-08-31 11:15] COMPLETED**
- Módszertani dokumentum írva: `_doc_/methodology_doc/6500_cluster_regime_methodology.md`
- Szekciók: Hipotézis, Módszertani döntések, Adatok, OOS validáció eredmények, Értelmezés, Overfitting-rizikó, Fázis 3 döntési javaslat
- Kulcs megállapítás: Cluster 1 short-bias szignifikáns OOS-ben (p=0.0017, Bonferroni-korrigálva), de hold-out teszt szükséges
- Döntési javaslat: Fázis 3 bevezetés feltételesen — short-regime szűrő az LGBM mellett, ha hold-out + stabilitás tesztek pass
- Átjelölve: `pr_t869_methodology_rationale`

Validálva: ruff check pass, pyright type hints OK (non-blocking), pytest N/A. Dátum: 2026-08-31
