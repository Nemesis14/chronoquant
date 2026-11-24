---
epic: epic_071
id: t914
title: Tanítsd a challenger modellpárt S/R feature-ökkel a champion mellé
assignee: modeling_agent
status: done
blocks: [t915]
blocked_by: [t912, t913]
---

## Goal
Challenger modellpár (long + short) a `config/models.json`-ben (pl. `lgbm_solusdt_{l,s}_fw60_2101_2607_sr`),
ugyanazon a snapshoton, split-en és alap feature-készleten, mint a champion `lgbm_solusdt_{l,s}_fw60_2101_2607`,
plusz a t913 által javasolt `feat_levels` feature-ök. Teljes pipeline (setup → analyze).

## Acceptance Criteria
- [ ] Új snapshot, ha a `quant_train` bővült (a régi champion snapshot immutable marad)
- [ ] Champion és challenger valid top10_lift összehasonlítás + FE riport: hoznak-e importance-t az új feature-ök
- [ ] Registry provenance teljes
- [ ] Champion artifactok és deploy változatlanok

## Notes
- [orchestrátor 2026-11-19, validátor észrevételek a t912-ből]
  - Snapshot: `05_create_snapshot.py --features-profile solusdt_fw60_sr --start "2021-01-01 00:00:00" --end "2026-07-31 00:00:00"`; `exclude_features` nélkül `SELECT *` → a 6 extra target (fw240, `tb_label_*`) is bekerül. A champion oszlopkészlet + feat_levels A/B-hez használj `exclude_features`-t, csak `long/short_mfe_fw60` target maradjon.
  - A challenger tábla `quant_train__solusdt_fw60_sr` sorhalmaza = champion `quant_train`; a champion snapshot (`solusdt_fw60_2101_2607__49466cd2`) 208 base feature-je soronként azonos.
  - A champion élő `quant_train` 27 régi alt-data oszlopot hordoz, a challenger nem — A/B-t nem érinti (a champion snapshot sem használja).
  - A `feat_levels` nem tárol detektor-verziót: config-változás → `sync_levels --full` + challenger tábla újraépítés.
  - Feature-válogatás: a t913 javaslata szerint (ha a t913 egy csoportot no-go-nak ítél, azt is dokumentáld, hogy bent hagytad-e).

- [orchestrátor 2026-11-19] **CANCELLED — nem futott.** A t913 placebo-tanulmány 0/36 GO; a user döntése (2026-11-19): (A) megállás, az epic negatív eredménnyel zárul. Lásd epic.md Final Conclusion és `_doc_/research/sr_placebo_study.md`.
