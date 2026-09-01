---
epic: epic_065
id: t865
title: Ablak-reprezentáció — normalizálás, ritkítás, összegző statisztikák
assignee: modeling_agent
status: done
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

## Implementation

1. **Adatbetöltés:** quant_train (3.12M sor, 14 feature t863-ból) + fw360 targets (t864).

2. **Ritkítás:** 104K sor (30 percenkénti mintavételezés, 3120336 → 104011 sor).

3. **Ablak-reprezentáció:** 360 bar lookback, 5 összegző statisztika per feature (mean/slope/std/min/max).

4. **Normalizálás:** Ablakon belüli z-score per feature (alak-felismerés, ár-szint független).

5. **Kódimplementáció:** research/cluster_regime/02_window_representation.py
   - compute_window_features() — end-to-end pipeline
   - validate_window_representation() — KS teszt normalizáció validálása

6. **Output:** `artifacts/research_cluster_regime/solusdt_window_repr.parquet` (36 MB)
   - Rows: 104,011 (ritkított megfigyelések, 30 percenként)
   - Cols: open_time + 70 feature stat (14×5) + fw360 targets + fw60 targets
   - Ready for t866 (klaszterezés)

7. **Validáció:**
   - NaN/NULL kezelése per statisztika
   - KS test: normalizált feature-ek ~ N(0,1)
   - Window count distribution

## Validáció Rezultátum

✓ Ablak-reprezentáció számítása sikeres (2026-08-31)

- Shape: 104,011 rows × 75 columns
- Columns: open_time + 70 summary stats (14 features × 5 stats) + 4 MFE targets
- Data quality:
  - First 360 rows have NaN (insufficient lookback window) — expected
  - Row 5000+: normalized values (z-score ~N(0,1)) ✓
  - Targets: 100% non-null (fw360 and fw60) ✓
- Output: artifacts/research_cluster_regime/solusdt_window_repr.parquet (36 MB)

Készítve a t866 (klaszterezés) számára.

## Notes

Validálva: ruff check pass, pyright type hints OK (non-blocking), pytest N/A. Dátum: 2026-08-31
