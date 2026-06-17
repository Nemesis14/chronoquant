---
epic: epic_009_doc_rename
id: t3
title: Sampling metodológiai tartalom — 3100 kibővítés + legacy cleanup
assignee: doc_agent
status: pr
---

## Goal

A `_doc_/3100_sampling.md` kódszintű dokumentáció volt. Kibővítettük üzleti és
módszertani tartalommal a legacy `docs/modeling/sampling.md` alapján, majd
jelöltük deprecated-nek a legacy fájlokat.

## Scope

- `_doc_/3100_sampling.md` — kibővítve módszertani szekcióval
- `.agent/skills/docs_skill.md` — X100 szintű docs methodology rule hozzáadva
- `docs/modeling/sampling.md` — deprecated jelölés, tartalom törölve
- `docs/data/datasets.md` — deprecated jelölés, tartalom törölve

## Acceptance Criteria

- [x] 3100_sampling.md tartalmaz: üzleti háttér, módszertani döntések (miért expanding window, miért embargo, miért chronologikus), sample ID policy, target NULL szemantika, paraméter-indoklás, final holdout philosophy, validációs checklist
- [x] docs_skill.md tartalmaz iránymutatást: X100 szintű fájlokba módszertani tartalom is kötelező
- [x] Legacy sampling.md és datasets.md deprecated jelölést kapott
- [x] A legacy tartalom nem mond ellent az új kódnak — az eltérő részeket nem vettük át

## Notes

A legacy `docs/modeling/sampling.md` kb. 95%-ban megfelelt az új kódnak. Kivétel:
a régi legacy fájlban `dataset.parquet` opcionális kimenetre volt utalás — ez az
új pipeline-ban nem releváns (nincs parquet export a sampling modulban), ezért
nem kerül be az új doksiba.
