---
epic: epic_072
id: t927
title: 5200 — a v2 ellenőrzőpont döntéseinek rögzítése (egyedi t0 OOS-minimum, W4h_H4h NO-GO)
assignee: methodology_agent
status: done
blocks: []
blocked_by: []
---

## Goal
A user 2026-11-20-án a t925 után döntött; rögzítsd a `_doc_/methodology_doc/5200_wave_start_image_model.md`-ben:
- Az OOS ≥ 100 minimum az **egyedi t0** eseményszámra vonatkozik (nem a cellák összegére, mert a két cella t0-i szinte azonosak).
  Az eredmény: 75 egyedi OOS esemény < 100 → az OOS előjel-ellenőrzés nem fut, a legjobb elérhető eredmény "feltételes GO".
  Javítsd minden helyen (diagram, paramétertábla, go/no-go, checklist), ahol "a két cellában együtt" / cella-összeg szerepel.
- A W4h_H4h cella a CV pooled minimum miatt NO-GO (66 train impulzus < 100); a t919/t920 csak a W4h_H8h cellán fut.
  Holm-család így 1 cella (Holm = korrigálatlan p) — írd le.
- Tiszta OOS-próba később a holdout-2-n (jelenleg 44 egyedi esemény), csak elég esemény után.

## Acceptance Criteria
- [ ] Nincs ellentmondó "cella-összeg" OOS-definíció a doksiban
- [ ] Döntésnapló sor a v2 changelogban (dátum, t925 számok)

## Notes
- 2026-11-20 (methodology_agent): `5200_wave_start_image_model.md` frissítve.
  - OOS-minimum egyedi t0-ra: a "két cellában együtt" / cella-összeg szövegek (overview-diagram, changelog, Min. eseményszám, go/no-go szekció + mermaid, paramétertábla, kockázat, checklist, rögzített döntések) javítva. Cella-összeg-alapú OOS-definíció nem maradt.
  - t925 számok: egyedi OOS t0 = 75 (< 100 → OOS-előjel nem fut, legjobb eredmény feltételes GO); holdout-2 jelenleg 44 egyedi esemény.
  - W4h_H4h NO-GO (CV-ben 66 train impulzus < 100); t919/t920 csak W4h_H8h-n fut. Holm-család 1 cella, korrigálatlan p (Holm = identitás) — a Holm-szövegek és a mermaid-címke ezt írják.
  - Döntésnapló sor a changelog táblában + rögzített döntések 7–8 (OOS-minimum, holdout-2).
  - Ticket nincs átnevezve, acceptance criteria nincs bepipálva.

**[validator] Validated - 2026-11-20 - PASS.** OOS minimum defined on unique t0 everywhere (changelog, min event count, go/no-go, mermaid, parameter table, risks, checklist, decisions); no operative cell-sum definition remains (remaining mentions explicitly negate it). W4h_H4h NO-GO (66 < 100) and single-cell Holm (identity) stated; changelog row present with t925 numbers (75 unique OOS, 44 holdout-2). Fix: epic.md Key Decisions v2 line still said ">=100 esemeny a 2 cellaban" - corrected to unique t0.
