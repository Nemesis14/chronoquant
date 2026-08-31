# PR t867 — Multi-year OOS validáció és multiple-testing korrekció

**Task**: Validate Cluster 2 direction bias out-of-sample (train/valid/OOS split)

**Epic**: epic_065_cluster_regime_strategy

**Status**: `pr_` — Implementation in progress

**Assignee**: automated (orchestrator delegation → direct implementation)

---

## Kontextus

Utána: t866 (K-means klaszter-modell illesztés train periódusra, iránybiasz azonosítás)

Elő: t868 (interpretáció és döntési pont)

---

## Esszenciális kérdések (megoldva)

### Q1: Van-e fitted K-means pickle/joblib objektum?

**A**: Nem. Az `03_clustering.py` nem menti el a fitted K-means objektumot, csak:
- `cluster_labels.csv` (train periódus cluster assignments + MFE értékek)
- `cluster_profiles.json` (train periódus klaszter statisztikák)

**Implikáció**: Az OOS scriptben **újra kell fit-elni a K-means modellt a train adatokon**, majd predict valid/OOS-re.

### Q2: Milyen periódus a train?

**A**: Train: 2021-01-01 – 2021-12-31 (17,520 sor = 365 nap × 48 bar/nap)

Data teljes tartomány: 2020-09-14 – 2026-08-21 (104,011 sor)

Periódusok:
- Train: 2021-01-01 – 2021-12-31 (17,520 sor)
- Valid: 2022-01-01 – 2022-12-31 (17,520 sor)
- OOS: 2023-01-01 – 2025-12-31 (52,608 sor)
  - 2023: 17,520 sor
  - 2024: 17,568 sor
  - 2025: 17,520 sor

### Q3: Milyen időzóna?

**A**: UTC (Binance OHLCV standard). Timestramp mikroszekundum pontosság (`timestamp[us]`).

---

## Implementáció lépések

1. **Train/Valid/OOS Split**: Dátumok alapján szétválasztás (fenti periódusok)

2. **K-means Fitting**: Train adatokra fit n_clusters=3, seed=42 (reprodukálható)

3. **Predict & Profiling**: Valid + OOS periódusokra cluster assignments, majd per-cluster profiling

4. **Multiple-Testing Korrekció**: Bonferroni correction
   - α_base = 0.05
   - Tesztek száma: 3 clusters × 2 directions = 6
   - α_corrected = 0.05 / 6 = 0.0083

5. **Output JSON-ek**:
   - `oos_validation_results.json`
   - `oos_cluster_profiles.json`

6. **Commit**: `research/cluster_regime/04_oos_validation.py` + JSON output-ok

---

## Siker kritériuma

**Kérdés**: Cluster 2 irány-biasza szignifikáns marad-e OOS-ben az α_corrected = 0.0083 küszöb alatt?

- **Igen** → regime stratégia lehetséges, Fázis 3 döntési pont
- **Nem** → data-dredging suspicion, negatív eredmény dokumentálása szükséges

---

## Notes (munkafolyamat)

- [x] Esszenciális kérdések megválaszolva
- [ ] `04_oos_validation.py` megírása
- [ ] Valid/OOS profiling és Bonferroni korrekció
- [ ] Output JSON-ek generálása
- [ ] Commit és test

