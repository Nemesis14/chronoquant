# Klaszter-rezsim OOS validáció (régi 6500 notebook, t867)

**Státusz:** kutatási eredmény, nem újragenerált. A bővebb, későbbi (k=10) munka: `6500_cluster_regime_analysis_v2.md`, `6501_*.md`, `6502_*.md`. Notebook + `6500_README.txt` törölve.

**Mit vizsgáltunk.** Egy k-means klaszter-alapú SOLUSDT setup (Cluster 1 SHORT) tradelhetőségét OOS (2023-2025) a 2021-es train profilhoz képest.

**Módszer.** Train (2021) klaszterprofilok, OOS profilok, train->OOS decay, évenkénti stabilitás, klaszterméret-stabilitás (CV), Newey-West effektív N, Bonferroni-korrigált szignifikancia.

**Eredmény.** Cluster 1 SHORT edge szignifikáns (p=0.0017, Bonferroni), átlagos OOS short MFE -0.022, effektív N=382; 2/3 évben szignifikáns (2025 a legerősebb, 2023 nem szignifikáns). Klaszterméret stabil (CV=0.30), Sharpe decay ~57% (várható).

**Döntés (akkori).** Feltételes tradelhető edge: Phase 3 csökkentett pozíciómérettel, negyedéves rezsim-monitorral, költségcél < 1 bp. A későbbi k=10 munka ezt finomította (lásd v2).
