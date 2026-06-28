# 2011 — Price Action Feature Analízis: MI bevezetése és akciók

**Modell:** `lgbm_solusdt_l_fw60_2101_2605` | **Szekció:** Price Action (16 feature)
**Elemzési alap:** `feature_engineering_scatter.ipynb` — train split, 37 939 sor

---

## Miért vezettük be a Mutual Information metrikát?

A feature engineering elemzés első iterációja Pearson-korrelációt (`r`) használt a feature–target kapcsolat mérésére. Ez szükséges, de nem elégséges metrika.

### A lineáris korreláció vak foltja

A Pearson `r` kizárólag **monoton lineáris összefüggést** kap el. Ha a kapcsolat nem-lineáris — például U-alakú, ahol mindkét szélső értéknél magas a target, a közepén alacsony — akkor a pozitív és negatív eltérések kioltják egymást, és az eredmény `r ≈ 0`.

Ez nem jelenti azt, hogy a feature prediktív ereje nulla. Azt jelenti, hogy a választott metrika nem alkalmas a mérésre.

**Konkrét példa a Price Action csoportból:**

| Feature | Pearson r | Valódi MI |
|---|---|---|
| `return_10` | 0.004 | **0.051** |
| `returns_log` | -0.011 | **0.048** |
| `return_60` | 0.002 | **0.046** |

A `return_10` lineárisan teljesen inert, de MI alapján a csoport 4. legerősebb feature-je — közel annyi prediktív információt hordoz, mint a `returns_std_14`.

### A Mutual Information előnye

A `sklearn.feature_selection.mutual_info_regression` k-NN alapú MI becslő:

- Nem feltételez funkcionális formát (lineáris, U-alak, küszöb, bármilyen egyéb)
- Bármilyen statisztikai függőséget kap el
- `0` = teljes függetlenség, magasabb érték = erősebb asszociáció
- Continuous targettel és continuous/binary feature-ekkel egyaránt működik

**Kapcsolat az optbinning IV-vel:** Az optbinning `ContinuousOptimalBinning` által számolt IV szintén képes nem-lineáris kapcsolatokat detektálni (binnenként nézi a target átlagot), de a continuous target esetén alkalmazott WoE-adaptáció egy saját, kis számokat produkáló skálán dolgozik. A két metrika **rangsora konzisztens**, de az MI értékek közvetlenül összehasonlíthatóbbak és interpretálhatóbbak.

A scatter notebook ezért mostantól mindkét metrikát tartalmazza, **MI szerint rendezve**.

---

## Miért nem irányt mér a `returns_std_14`?

`returns_std_14` = log-return gördülő szórása az előző 14 báron (ddof=1).

Ez a Price Action csoport MI-ben 3. legerősebb feature-je (`MI = 0.093`, `r = 0.406`). Az összefüggés mechanizmusa:

```
returns_std_14 (múlt 14 bár szórása)
        ↓  [volatilitás-autokorrelació — GARCH-szerű, kripto piacokon erős]
future volatilitás (következő 60 bár)
        ↓  [nagy volatilitásban nagy elmozdulás lehetséges]
long_mfe_fw60 magas  AND  short_mfe_fw60 magas
```

**A kulcsmegfigyelés:** a target `long_mfe_fw60` a következő 60 bárban elért *maximum felfelé irányuló elmozdulás*. Nagy múltbeli volatilitás → nagy jövőbeli volatilitás → az ár messzebbre tud mozdulni bármilyen irányba → az MFE (maximum favorable excursion) is nagyobb lesz.

Ez az összefüggés **irány-agnosztikus**: ugyanennyire igaz a short modell `short_mfe_fw60` targetjére is. A feature nem azt mondja meg, hogy *felfelé* megy az ár, hanem hogy *mekkora elmozdulás* várható.

**Következmény a modellezésre:** ez a feature nem a belépési irányt kalibrálja, hanem a várható MFE nagyságrendjét. A search és fit során ezért hasznos, de önmagában nem differenciál long vs short döntés között. A long vs short döntést más csoportok feature-ei hordozzák (Momentum, Trend, Candle Pattern stb.).

---

## Redundáns feature-párok és javaslatok

### 1. `hml_range` ≡ `ohlc_range` (r = 1.000)

| Feature | Képlet | MI |
|---|---|---|
| `hml_range` | `(high - low) / close` | 0.0986 |
| `ohlc_range` | `(high - low) / ((open + close) / 2)` | 0.0982 |

A nevező különbözik (`close` vs OHLC-átlag), de kripto perces báron a két érték szinte azonos — a korreláció kerekített 1.000. Teljesen azonos információt hordoznak.

**Döntés: `ohlc_range` törlése.**

Indoklás: a `hml_range` képlete egyszerűbb és jobban interpretalható (`(H-L)/C` = a gyertya range-je a záró ár százalékában). Az `ohlc_range` komplexebb nevező-definíciója nem hoz semmilyen addicionális tartalmat.

### 2. `return_z_W` ≡ `vol_adj_return_W` (r = 1.000 minden ablakra)

| Feature | Képlet | MI |
|---|---|---|
| `return_z_10` | `ret1 / rolling_std(log_ret, 10)` | 0.0037 |
| `vol_adj_return_10` | `ret1 / rolling_std(log_ret, 10)` | 0.0038 |
| `return_z_30` | `ret1 / rolling_std(log_ret, 30)` | 0.0000 |
| `vol_adj_return_30` | `ret1 / rolling_std(log_ret, 30)` | 0.0000 |

**Ez nem közel-azonos, hanem pontosan azonos képlet.** A két feature neve különbözik (Z-score vs Sharpe-proxy per bar framing), de a számított érték bit-for-bit megegyezik. Ez adatgenerálási hiba — a `_features_polars.py`-ban ugyanaz a transzformáció kétszer lett elnevezve.

**Döntés: `vol_adj_return_10` és `vol_adj_return_30` törlése.**

Indoklás: a `return_z_W` elnevezés pontosabb — Z-score normalizáció standard fogalom, könnyen interpretalható. A `vol_adj_return` név félrevezető ("volatility-adjusted return" — de ugyanaz, mint a Z-score).

---

## MI = 0 feature-ök: törlési javaslatok

Az alábbi feature-ökre a Mutual Information `0.0000`-t mutat — azaz **statisztikailag nem detektálható asszociáció** a targettel:

| Feature | MI | Megjegyzés |
|---|---|---|
| `return_z_30` | 0.0000 | Ráadásul `vol_adj_return_30`-cal azonos képlet |
| `return_z_60` | 0.0000 | |
| `vol_adj_return_30` | 0.0000 | `return_z_30` duplikátuma |

A 30 és 60 bár ablakú return-Z értékek semmilyen prediktív tartalmat nem hordoznak a `long_mfe_fw60` targetre. A 10 bár változat (`return_z_10`, `MI = 0.0037`) szintén nagyon gyenge, de nem nulla.

**Döntés: mindhárom törlése a DB-ből és a `features.json` konfigurációból.**

### Határeset: `returns_kurt_14`

| Feature | MI | Megjegyzés |
|---|---|---|
| `returns_kurt_14` | 0.0004 | Lényegében nulla |

A 14 bári gördülő kurtózis (`Fisher, excess`) elvben megragadhat farokkockázati mintázatokat, de a jelenlegi adaton nincs mérhető haszna. Mérlegelendő törlés — de nem sürgős, mivel a modell a fa-alapú struktúrájából adódóan nem büntetődik erősen felesleges feature-ért.

---

## Összefoglaló akciólista

| # | Akció | Feature(ek) | Ok |
|---|-------|-------------|-----|
| 1 | **Törlés** | `ohlc_range` | Duplikátuma `hml_range`-nek (r=1.000), képlete bonyolultabb |
| 2 | **Törlés** | `vol_adj_return_10`, `vol_adj_return_30` | Azonos képlet mint `return_z_10`/`return_z_30` — adatgenerálási duplikáció |
| 3 | **Törlés** | `return_z_30`, `return_z_60` | MI = 0, nincs prediktív tartalom |
| 4 | **Mérlegelendő törlés** | `returns_kurt_14` | MI ≈ 0 (0.0004), marginális jel |

**Érintett fájlok:** `src/data_handling/sync_tables/_features_polars.py`, `config/features.json`

**5 definit törlés** esetén a Price Action csoport 16 → 11 feature-re csökken, az összes elveszített információ minimális (az eltávolított feature-ök MI-je összesen < 0.005).

---

## Megjegyzés a módszertanra

Ez az analízis a `lgbm_solusdt_l_fw60_2101_2605` modell train split-jén futott. Az MI-értékek modell- és adatperiódus-specifikusak — más modelleknél (pl. short irány, más timeframe) eltérhet a sorrend. A törlési döntések előtt érdemes cross-validálni, hogy a `short` modell scatter-je is ugyanezt az MI-képet mutatja-e az érintett feature-öknél.
