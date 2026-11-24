---
epic: epic_072
id: t921
title: Foglald össze a kutatást és hozz go/no-go döntést a modell-koncepcióra
assignee: analyst_agent
status: done
blocks: []
blocked_by: [t920]
---

## Goal
`_doc_/research/wave_image_clustering.md`: módszer, eredmények, go/no-go, és go esetén javaslat a teljes
modell-koncepcióra (önálló modell vagy gate/feature a fw60 LGBM-hez). Az epic.md Key Decisions frissítése.

## Acceptance Criteria
- [ ] md összefoglaló a research zónában, a README index frissítve

## Notes

**2026-11-20 — analyst_agent (kész, a ticket nincs átnevezve)**
- Új: `_doc_/research/wave_image_clustering.md` (cél, módszer v1->v2, eredmények t918/t925/t919/t920, verdikt, értelmezés, korlátok, opciók, újragenerálás); index sor a `_doc_/research/README.md`-ben; dátumozott Key Decision sor az `epic.md`-ben (az epic státusza OPEN maradt, a user dönti).
- Verdikt: képkoncepció NO-GO (W4h_H8h, 3. feltétel: p = 0,2617). Skalár alap AUC 0,6647 (p 0,001), ΔAUC +0,0287, CI [-0,0117; +0,0685].
- Folytatás csak opcióként: (a) lezárás; (b) skalár S0+C mint gate/feature a fw60 LGBM-hez, külön előre rögzített teszttel és holdout-2-vel (érintetlen, 44 egyedi esemény).
- Új számítás nem futott; minden szám a done_ ticketek Notes-ából és a három riportból származik.

**[validator] 2026-11-20 — PASS (doc-only).** Minden szám visszaellenőrizve a wave_event_dataset / wave_image_clusters / wave_predictiveness_gate tables/*.json fájlokkal és a done_ ticket Notes-szal: egyezik. Verdikt NO-GO összhangban az 5200 v2 3. feltételével (p = 0,2617 >= 0,01); opciók csak opciók; holdout-2 érintetlen; epic OPEN. Újragenerálási parancsok egyeznek a builder CLI-kkel (`--group research` a torch/tslearn buildereknél). Javítva: (1) "minták ~85%-a oldalazás" -> 84-87% (W4h_H8h: 68%); (2) klaszter-állítás pontosítva (C8 18,4% kivétel, nem "legalacsonyabb" mind C5-C9).
