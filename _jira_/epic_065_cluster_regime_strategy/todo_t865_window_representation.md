---
epic: epic_065
id: t865
title: Ablak-reprezentáció — normalizálás, ritkítás, összegző statisztikák
assignee: modeling_agent
status: todo
blocks: [t866]
blocked_by: [t863]
---

## Goal

Minden (ritkított) megfigyeléshez előállítani a 360×10-es visszatekintő ablak
klaszterezhető reprezentációját: a t863-ban kiválasztott feature-készletre
normalizálás + összegző statisztikák (kezdő megközelítés: mean/slope/std/min/max
soronként, nem raw 3600-dim vektor).

Módszertani háttér: [`_doc_/_plans_/plan.md`](../../_doc_/_plans_/plan.md) 3.1, 3.3, 3.4 szakasz.

## Scope

- Mintavételi sűrűség: **ritkítva, 15-30 percenként** (eldöntött, lásd epic.md
  Key Decisions) — nem minden perc.
- Normalizálás: **kizárólag alak-felismerés** — ár-dimenzió log-return relatív az
  ablak elejéhez; minden más feature ablakon belüli z-score vagy percentil-rang.
  Ár-szint és volatilitás-rezsim független reprezentáció (eldöntött).
- Reprezentáció: Opció A a plan.md 3.4 táblázatából (összegző statisztikák
  ablakonként/soronként) — PCA/autoencoder vagy DTW-alapú finomítás csak akkor,
  ha ez az egyszerűbb megközelítés edge-et mutat (lásd plan.md javaslat).
- Kimenet: `research/cluster_regime/window_features.parquet` (vagy hasonló) —
  egy sor / ritkított megfigyelés, oszlopok = az összegző statisztikák.

## Acceptance Criteria

- [ ] Ritkított mintavétel implementálva (15-30 perces lépésköz, konfigurálható)
- [ ] Normalizálás implementálva és validálva (pl. random mintán vizuálisan
      ellenőrizve, hogy a különböző ár-szintű időszakok összemérhetők)
- [ ] Összegző statisztikák számítva a t863-ban kiválasztott feature-készletre
- [ ] Eredmény tábla dokumentálva, elérhető a t866 (klaszterezés) számára

## Notes
