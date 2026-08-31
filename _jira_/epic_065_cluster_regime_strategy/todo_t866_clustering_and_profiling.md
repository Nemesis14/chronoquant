---
epic: epic_065
id: t866
title: Klaszterezés implementálása és profilozás
assignee: modeling_agent
status: todo
blocks: [t867]
blocked_by: [t864, t865]
---

## Goal

A t865 ablak-reprezentációra klaszterezést illeszteni egy kisebb, egyetlen éves
mintán (gyors iteráció), majd minden klaszterre profilozást futtatni a t864-ben
számított `fw360` MFE targetekkel: méret, irány-bias (long/short), effektív N,
bootstrap konfidencia-intervallum.

Módszertani háttér: [`_doc_/_plans_/plan.md`](../../_doc_/_plans_/plan.md) 3.5, 3.7 szakasz,
4.1-4.2 kritikai pontok (effektív N, data dredging).

## Scope

- Elsődleges algoritmus: **HDBSCAN** (van "noise" osztálya — nem kényszerít minden
  pontot klaszterbe). Baseline összehasonlításként k-means és/vagy GMM.
- Klaszterenkénti profil: `n_obs` (nyers), **effektív N** (nem-átfedő/független
  esemény-becslés — lásd plan.md 4.1), átlag/medián/szórás `long_mfe_fw360` és
  `short_mfe_fw360`, bootstrap CI.
- Egyetlen éves mintán fut ebben a taskban (gyors iteráció) — a teljes
  2021-2025 multi-year validáció a t867 taskban történik.
- Kimenet: `research/cluster_regime/cluster_profiles.parquet` vagy `.json` +
  a klaszter-modell szerializálva (újrafelhasználáshoz a t867 OOS assign-hoz).

## Acceptance Criteria

- [ ] HDBSCAN klaszterezés lefutott a t865 reprezentáción, legalább egy éves mintán
- [ ] k-means/GMM baseline összehasonlítás dokumentálva
- [ ] Minden klaszterre kiszámolva: n_obs, effektív N, MFE átlag/medián/CI mindkét irányba
- [ ] Effektív N számítási módszer explicit dokumentálva (nem nyers sorszám)
- [ ] A klaszter-modell elmenthető/betölthető formában (t867 predict-only assign-hoz)

## Notes
