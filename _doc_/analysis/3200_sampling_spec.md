# Analysis Spec — Sample EDA: `solusdt_fw60_2010_2605`

**Notebook:** `3200_sampling.ipynb`
**HTML output:** `_doc_/3200_sampling.html`
**Sample:** `solusdt_fw60_2010_2605`
**DB:** `database/solusdt/solusdt.duckdb`
**Feature table:** `feat_ohlcv_quant`
**Target table:** `target`
**Targets:** `trg_l_fw60_q90` (long), `trg_s_fw60_q10` (short)

---

## Objective

Modellezés előtti teljes sample vizsgálat: igazolja, hogy a `solusdt_fw60_2010_2605`
sample minőségileg alkalmas LightGBM binary classifier fejlesztésre. Nem test, hanem
feltáró elemzés — minden szekció kvantitatív eredménnyel és szöveges értékeléssel zárul.

---

## Setup

```python
import duckdb, json, pathlib
import polars as pl
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from scipy import stats
```

- Load `folds.json` és `metadata.json` a `database/solusdt/samples/solusdt_fw60_2010_2605/` mappából.
- Kapcsolódj a DuckDB-hez read-only módban.
- Töltsd be a teljes `feat_ohlcv_quant` + `target` JOIN-olt táblát Polars DataFrame-be
  (csak a szükséges oszlopok: `open_time`, összes `feat_*`, `trg_l_fw60_q90`, `trg_s_fw60_q10`).
- Rendeld hozzá minden sorhoz, hogy melyik fold split-jébe esik:
  `train_fold_N`, `valid_fold_N` vagy `test` vagy `pre_sample` (sample range-en kívül).
- Dolgozz végig a DataFrame-mel — ne írj vissza a DB-be.

---

## 1. Fold áttekintés — sorok és target eloszlás

### 1.1 Sor- és időszak statisztikák per fold

Táblázat minden fold-hoz (train és valid külön):

| Fold | Split | Időszak start | Időszak end | Sorok | Sorok (NULL target nélkül) |
|------|-------|---------------|-------------|-------|---------------------------|

Mindkét target-re (`trg_l_fw60_q90`, `trg_s_fw60_q10`).

### 1.2 Target positive rate per fold

- Kiszámítandó: `sum(target==1) / count(target is not null)` per fold, per split (train / valid).
- Vizualizáció: grouped bar chart — foldok az x-tengelyen, train vs valid csíkok,
  vízszintes referenciasáv 8–12% (elvárt tartomány a q90/q10 targetekre).
- Mindkét targetre külön plot.
- **Szöveges értékelés:** a positive rate a várt tartományban van-e? Jelezd, ha valamelyik
  fold vagy split outlier.

### 1.3 Train méret növekedés (expanding window vizualizáció)

- Vízszintes sávdiagram: minden fold train ablaká + valid ablaká az időtengelyen.
- Mutasd a test set pozícióját is.
- Az expanding window karakterének ellenőrzése: train mindig data_start-tól indul.

---

## 2. Feature eloszlások és drift

### 2.1 Feature csoportok

A `feat_ohlcv_quant` tábla feature-eit csoportosítsd prefix alapján:

- **momentum:** `feat_*rsi*`, `feat_*macd*`, `feat_*mom*`, `feat_*roc*`
- **volume:** `feat_*vol*`, `feat_*obv*`, `feat_*vwap*`
- **volatility:** `feat_*atr*`, `feat_*bb*`, `feat_*std*`, `feat_*range*`
- **egyéb:** maradék `feat_*`

### 2.2 NULL arányok per feature

- Táblázat: feature neve, NULL arány a teljes sample-ben, NULL arány per fold valid set-ben.
- Jelölj meg minden feature-t, ahol a NULL arány > 1% (a vezető rolling window NULL-okon túl).
- Windowed feature leading null ellenőrzés: minden `feat_*` esetén a leading NULL-ok száma
  konzisztens-e a rolling window-zal? (pl. ha egy feature 14-bares RSI, pontosan 13 vezető NULL kell.)

### 2.3 Eloszlások per fold (válogatott feature-ök)

- Csoportonként 3-3 reprezentatív feature-t válassz (összesen ~12 feature).
- Per feature: violin plot vagy KDE — minden fold valid split-je külön sáv/görbe.
- Cél: eloszlás-drift vizuális feltárása foldok között.
- **Szöveges értékelés:** látható-e szisztematikus drift? Melyik feature és melyik fold-nál?

---

## 3. Fold összehasonlítás

### 3.1 Statisztikai összefoglaló per fold

Minden fold valid set-jére, minden numerikus feature-re: mean, std, median, IQR, skew.
Aggregált drift metrika: per feature az összes fold medián-jának szórása
(`std(median_fold_1 … median_fold_5)`). Top 20 leginkább drift-elő feature táblázatba.

### 3.2 Distribution shift teszt

- Minden fold valid set-jét hasonlítsd az első fold valid set-jéhez (referencia).
- Teszt: Kolmogorov–Smirnov statisztika per feature, per fold-pár.
- Heatmap: feature × fold, KS statisztika értéke, p < 0.05 cellák jelölve.
- **Szöveges értékelés:** melyik feature mutat szignifikáns drift-et? Módszertanilag
  aggályos-e (pl. VWAP-jellegű price-level feature)?

---

## 4. Feature korrelációk

### 4.1 Teljes korrelációs mátrix

- Pearson korrelációs mátrix az összes `feat_*` feature-re, a teljes train set-en
  (összes fold train részeinek uniója, NULL sorok droppolva).
- Vizualizáció: clustered heatmap (seaborn clustermap, Ward linkage).
- Annotáld a > 0.9 abszolút korreláció cellákat.

### 4.2 Magas korrelációjú feature párok

- Listázd ki az összes feature párt ahol `|r| > 0.90`.
- Táblázat: feature_a, feature_b, pearson_r — csökkenő sorrendben.
- **Szöveges értékelés:** ez multikollinearitási kockázatot jelent-e LightGBM-nél?
  (LightGBM általában robusztus rá, de erős korreláció feature importance instabilitást okozhat.)

### 4.3 Feature–target korreláció

- Spearman korreláció minden `feat_*` és mindkét target között.
- Top 20 feature per target (abszolút Spearman szerint), bar chart.

---

## 5. Feature csoport korrelációk

### 5.1 Csoporton belüli korrelációk

Minden csoportra (momentum, volume, volatility, egyéb):
- Átlagos pairwise Pearson korreláció a csoporton belül.
- Heatmap a csoport feature-eiről.

### 5.2 Csoportközi korrelációk

- Csoportonkénti átlag feature-t képezz (PCA helyett egyszerű mean normalizált
  feature értékekből).
- 4×4-es korrelációs mátrix a 4 csoport átlag-feature-e között.
- **Szöveges értékelés:** melyek a legerősebb cross-group összefüggések?

---

## 6. Test set hasonlóság a CV-hez

### 6.1 Feature eloszlás — test vs valid foldok

- Minden válogatott feature-re (2.3-as szekció ugyanazon ~12 feature): KDE overlay plot.
  Görbék: fold_1_valid … fold_5_valid + test (különálló szín).
- Cél: a test set eloszlása a CV valid foldok tartományán belül van-e,
  vagy extrapolál (distribution shift a modell éles értékelési periódusán)?

### 6.2 KS távolság — test vs minden fold

- KS statisztika: test set vs fold_N valid set, minden feature-re.
- Heatmap: feature × fold, KS érték. Legfelső sor: test vs összes fold átlaga.
- **Szöveges értékelés:** a test set mennyire "ismerős" a CV valid foldokhoz képest?
  Ha a test erősen outlier minden foldhoz képest, az az élesítés kockázatát jelzi.

### 6.3 Target positive rate — test vs CV

- Test set positive rate (mindkét target) vs CV fold valid átlag + szórás.
- 1 sor táblázat + szöveges értékelés.

---

## 7. Metodológiai ellenőrzések (ML checklist)

### 7.1 Embargo gap ellenőrzés

- Minden fold-ra: `MIN(valid_open_time) - MAX(train_open_time)` percben.
- Elvárt minimum: 60 perc (target_horizon_minutes).
- Táblázat: fold, tényleges gap (perc), megfelel-e.

### 7.2 Duplikált `open_time` értékek

- A teljes JOIN-olt táblában duplikált `open_time` count.
- Per fold és per split is ellenőrzendő.

### 7.3 Feature availability timestamp

- `feat_ohlcv_quant.open_time` = az adott perc nyitó timestampje.
- Ellenőrzés: a feature-ök számítása kizárólag a zárt (t-1) gyertya adataira épül-e?
  Ez nem közvetlenül queryezhető, de dokumentáld a projekt t-1 lag szabályát és
  utalj az audit.json adataira.

### 7.4 NULL target szemantika ellenőrzés

- A `trg_l_fw60_q90` és `trg_s_fw60_q10` NULL értékek kizárólag az utolsó 60 bar-ban
  jelennek-e meg? Ellenőrzés: NULL-ok időbeli pozíciója a táblában.
- Ha NULL-ok máshol is vannak: flag az analyst agentnek, todo ticket a modeling agentnek.

---

## 8. Summary

A notebook utolsó cellájaként szöveges összefoglaló:

- Összesen hány sor, hány fold, dátumtartomány.
- Target positive rate rendben van-e mindkét targetnél?
- Látható-e szisztematikus feature drift a foldok között? (igen/nem + melyik feature-csoportban)
- Magas korreláció: hány feature pár > 0.90? Kockázat LightGBM-re?
- Test set mennyire hasonló a CV valid foldokhoz? (alacsony / közepes / magas drift)
- Metodológiai ellenőrzések: minden átment, vagy van blocker?
- **Végső ítélet:** a sample alkalmas-e modellezésre, vagy szükséges beavatkozás?

---

## Elfogadási kritériumok

- [ ] Minden szekció (1–8) lefutott, nincs placeholder szöveg
- [ ] Mindkét target elemezve van
- [ ] Quarto render sikeres, HTML exportálva `_doc_/3200_sampling.html`-be
- [ ] Summary szekció kvantitatív számokra hivatkozik, nem általánosságokra
- [ ] Ha blocker found: `todo_` ticket létrehozva a megfelelő agentnek
