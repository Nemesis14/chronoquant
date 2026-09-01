---
epic: epic_065
id: t866
title: Klaszterezés implementálása és profilozás
assignee: modeling_agent
status: done
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

- Algoritmus: **K-means** (egyszerű, determinisztikus, gyors, OOS-ben könnyű predikt)
- Klaszterenkénti profil: `n_obs` (nyers), **effektív N** (nem-átfedő/független
  esemény-becslés — lásd plan.md 4.1), átlag/medián/szórás `long_mfe_fw360` és
  `short_mfe_fw360`, t-teszt szignifikancia.
- Egyetlen éves mintán fut ebben a taskban (gyors iteráció) — a teljes
  2021-2025 multi-year validáció a t867 taskban történik.
- Kimenet: `cluster_labels.csv` + `cluster_profiles.json` + `clustering_metadata.json`

## Acceptance Criteria

- [x] K-means klaszterezés lefutott a t865 reprezentáción, 104K mintán
- [x] Paraméter-kiválasztás via elbow/silhouette (k=3 optimális)
- [x] Minden klaszterre kiszámolva: n_obs, effektív N, MFE átlag/CI mindkét irányba
- [x] Effektív N számítási módszer: autokorrelációs súlyozás, nem nyers sorszám
- [x] Kimeneti fájlok: cluster_labels.csv (labels per observation), cluster_profiles.json (per-cluster stats)

## Completion Summary (2026-08-31)

**Algorithm:** K-means with k=3 (selected via silhouette scoring on 10K sample, fitted on full 104K)

**Results:**
- **Cluster 0** (n=8,706, 8.4% of data)
  - Long MFE: mean=0.0206, n_eff=326, p=0.957 (not significant)
  - Short MFE: mean=-0.0211, n_eff=309, p=0.595 (not significant)
  - Interpretation: Neutral cluster, no directional bias

- **Cluster 1** (n=80,087, 77.0% of data)
  - Long MFE: mean=0.0199, n_eff=2,592, p=0.152 (not significant)
  - Short MFE: mean=-0.0208, n_eff=2,999, p=0.036 ✓ significant at 95% CI
  - Interpretation: Significant SHORT bias — worse outcomes for short positions

- **Cluster 2** (n=15,218, 14.6% of data)
  - Long MFE: mean=0.0238, n_eff=464, p=0.036 ✓ significant at 95% CI
  - Short MFE: mean=-0.0273, n_eff=403, p=0.004 ✓ highly significant
  - Interpretation: Strong bidirectional bias — better for LONG, worse for SHORT

**Key Implementation Details:**
- Effective N calculation: accounts for autocorrelation via lag-1 to lag-50 decay
- Parameter selection: used 10K sample for silhouette scoring (O(N) reduction)
- Deterministic: random_state=42 for reproducibility
- Output files:
  - `artifacts/research_cluster_regime/cluster_labels.csv` (104K rows, open_time + cluster_id + targets)
  - `artifacts/research_cluster_regime/cluster_profiles.json` (per-cluster stats + significance)
  - `artifacts/research_cluster_regime/clustering_metadata.json` (algorithm params + metrics)

**Notes:**
- Original spec (todo) requested HDBSCAN; switched to K-means per direct task request (deterministic, OOS-friendly, fast)
- Effective N much lower than raw N in small clusters (0, 2) indicates high autocorrelation — expected for 30-min downsampled time series
- Cluster 1 dominates (77%) but shows significant short bias only — may be a regime where shorts underperform
- Cluster 2 (15%) shows strongest bidirectional signal — candidate for early attention in OOS validation (t867)
- Ready for OOS multi-year validation in t867

## Commit

```
c4549a7 t866: Implement K-means clustering on window representation
```

- `research/cluster_regime/03_clustering.py`: Implementation
- `artifacts/research_cluster_regime/cluster_labels.csv`: Cluster assignments + targets
- `artifacts/research_cluster_regime/cluster_profiles.json`: Per-cluster profiling statistics
- `artifacts/research_cluster_regime/clustering_metadata.json`: Algorithm metadata + metrics

Validálva: ruff check pass, pyright type hints OK (non-blocking), pytest N/A. Dátum: 2026-08-31
