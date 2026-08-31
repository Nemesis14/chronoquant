---
epic: epic_065
id: t867
title: Multi-year OOS validáció és multiple-testing korrekció
assignee: modeling_agent
status: todo
blocks: [t868, t869]
blocked_by: [t866]
---

## Goal

A t866-ban illesztett klaszter-modellt train perióduson rögzíteni, a valid/OOS
periódus megfigyeléseit hozzárendelni (predict, nem refit), és ellenőrizni, hogy a
train-en talált irány-bias megmarad-e OOS-en. Ez a lépés dönti el, hogy van-e
valódi edge vagy csak data dredging.

Módszertani háttér: [`_doc_/_plans_/plan.md`](../../_doc_/_plans_/plan.md) 3.8 szakasz,
4.2-4.3 kritikai pontok (data dredging, rezsimváltás).

## Scope

- Train/valid split OOS ellenőrzés a teljes 2021-2025 adatsoron (projekt-konvenció
  szerinti split, hasonlóan a jelenlegi LGBM champion modellek train/valid ablakához).
- Multiple-testing korrekció (Bonferroni vagy FDR) a szignifikáns klaszterek listáján.
- **Nyitott döntés ebben a taskban**: globális vs. évenkénti klaszterezés — ha az
  OOS eredmény azt mutatja, hogy a globálisan illesztett klaszterek nem stabilak
  a rezsimváltások miatt (plan.md 4.3), váltás walk-forward újra-illesztésre és
  ennek dokumentálása a Notes szekcióban.
- Kimenet: `research/cluster_regime/oos_validation_report.json` (vagy hasonló) —
  klaszterenként: train-bias, OOS-bias, effektív N mindkét periódusban, korrigált
  szignifikancia.

## Acceptance Criteria

- [ ] Klaszter-modell train perióduson rögzítve, OOS megfigyelések hozzárendelve
      (nincs refit az OOS adaton)
- [ ] Multiple-testing korrekció alkalmazva a szignifikáns klaszterek szűrésénél
- [ ] Explicit lista: mely klaszterek maradnak szignifikánsak OOS-en is (ha van ilyen)
- [ ] Globális vs. évenkénti klaszterezés döntése dokumentálva indoklással
- [ ] Eredmény strukturáltan elmentve, elérhető a t868 (interpretáció) és t869
      (rationale) taskoknak

## Notes
