# Klaszter-rezsim kutatás (k-means a 360 perces ablakokon) — epic_065

> **Verdikt: NEM KERESKEDHETŐ.** Van statisztikailag kimutatható irány-szeparáció a klaszterek között, de a klaszterek instabilak, és a train-ben látott irány a valid mintán a párok 69%-ában gyengül vagy előjelet vált.
>
> Időszak: 2021-01 – 2026-06 · SOLUSDT · futtatva 2026-08-31 – 09-01 (t863–t872)

## Mit néztünk
Létezik-e visszatérő piaci rezsim (klaszter), amelyhez tartósan egy irány (long vagy short) kötődik, tehát rezsim-alapú belépő szabály építhető.

## Módszer
14 alacsony redundanciájú feature (Pearson |r| < 0,6, kizárólag a 2021–22-es train ablakon választva), 360 perces lookback ablakonként ablakon belüli z-score + 5 összegző statisztika (mean, slope, std, min, max) = 70 dimenzió, 30 perces léptetéssel (~104 ezer sor). Cél: `long_mfe_fw360 + short_mfe_fw360` (6 órás előremutató MFE). MiniBatchKMeans, k = 2..30, 4 fold-os walk-forward CV (train 2021–22 → valid 2023; → 2024; → 2025H1; → 2025H2–2026H1), metrika: a valid mintán mért ANOVA F. Irány-hozzárendelés páros t-teszttel, Bonferroni (0,05/k).

## Fő számok
- Optimális k = 13, cv_score (átlagos F) 17,7, de a foldonkénti F 27,3 / 5,8 / 23,4 / 14,3 — nagy szórás (CV 0,54).
- Train→valid irány-stabilitás (13 klaszter × 4 fold = 52 eset): csak 16 egyezik (30,8%), 6 előjelet vált, 10 gyengül, 14 erősödik.
- Cross-fold klaszter-azonosság (centroid-koszinusz, mutual best match; megbízható: medián hasonlóság 0,998 vs. 0,963 fold-n belüli alap): 11 többfoldos minta, ebből 6 konzisztens short, 1 konzisztens long, 4 ellentmondó; 5 klaszter csak egy foldban él.
- A korábbi k=10 változat (t866–t871) költséges, valósághű végrehajtás mellett nem hozott kereskedhető élt (lásd `_doc_/models_doc/6501_*`, `6502_*`, `cluster_regime_oos_validation.md`).

## Döntés / következmény
Az epic_065 lezárva, rezsim-stratégia nem épül. A konzisztens short-minták (6) legfeljebb rezsim-szűrő ötletként jöhetnek elő egy másik stratégiában. A `research/cluster_regime/` kód és az `artifacts/research_cluster_regime/` kimenetek (35 MB: k-search JSON-ok, KMeans pkl, 104k soros ablak-parquet) törölve.

## Kód a git historyban
Commit `73f2156` (törlés előtti állapot): `research/cluster_regime/01_target_computation.py` … `07_cross_fold_signal_identity.py`. A kimenetek (`artifacts/research_cluster_regime/`) git-ignoráltak voltak, nem visszaállíthatók; a fenti számok a JSON-okból származnak. Részletes jelentés: `_doc_/models_doc/6502_cv_cluster_separation_optimization.md`.
