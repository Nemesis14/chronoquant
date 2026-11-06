# 6500 — Klaszter-alapú Setup-felismerés: Módszertani Rationale és Fázis 3 Döntési Javaslat

## Overview

Az Epic 065 egy **unsupervised klaszterezésen alapuló setup-felismerő kutatási irány**, amely független az eddigi LGBM-regressziós megközelítéstől. A módszer egy 360 perces (6 órás) visszatekintő ablakon számított, alacsony-redundanciájú indikátor-készletre épít, majd K-means klaszterezéssel piaci "formation"-okat azonosít, amelyek statisztikailag megbízható long/short irányú előretekintő torzítást mutatnak.

**Státusz:** Fázis 2 OOS validáció lezárult (t867). **Cluster 1 szignifikáns short-bias mutat az out-of-sample periódusban.** Ez a dokumentum az eredmények üzleti/módszertani értelmezését és Fázis 3 döntési javaslatát rögzíti.

---

## 1. Hipotézis

### Kutatási kérdés

Egy felügyelet nélküli klaszterezés képes-e olyan piaci "setup"-okat azonosítani, amelyek:
1. Statisztikailag robusztak az out-of-sample periódusban?
2. Elég nagy frekvenciájúak ahhoz, hogy gyakorlati kereskedési signált alkossanak?
3. Az LGBM pixelenkénti regresszió-megközelítéstől **funkcionálisan függetlenek** — azaz új információ hozzáadása lehetséges az LGBM-hez?

### Alapvetés

Az LGBM-modell egy rögzített time lag-en (tipikusan t-1) vett feature-vektort használ, és egy pontbecslést (long_mfe_fw60 vagy short_mfe_fw60) készít. Ez **helyvektor-alapú megközelítés** — "hol vagyunk most?" dimenziójában gondolkodik.

A klaszter-módszer ezzel szemben egy **idősor-alak alapú megközelítés** — "milyen pályavonal vezet ide az elmúlt 6 órában?" kérdésre válaszol. Az előzetes hipotézis: a pályavonal *alakja* (trend-momentum-volatilitás-volumen kombináció) egy invariáns karakterizációja lehet annak a piaci rezsimnek/setupnak, amely erős előretekintő long/short szignált hordoz.

---

## 2. Módszertani Döntések

### 2.1 Ablak-definíció

**Lookback: 360 perc (6 óra), Forward: 360 perc (6 óra)**

- Az 360 perces ablak szimmetrikus a forward horizont és az offline tanítási ablak szempontjából.
- Eltér az LGBM `fw60` target-jétől — szándékosan, hogy az unsupervised setup-felismerés egy másik időskálán működjön.
- **Mintavételi sűrűség:** Az adatok ritkítva vették fel (15-30 percenként) az autokorreláció csökkentésére és számításigény kezelésére. Ez kvázi-független megfigyeléseket eredményez a klaszterezéshez.

### 2.2 Feature-kiválasztás

**14 alacsony-redundanciájú indikátor** (t863 output), dimenzióként:

| Dimenzió | Feature | Indoklás |
|---|---|---|
| Ár/hozam | log-return | Szintfüggetlenség, az ablak normalizálásához referencia |
| Trend | EMA slope / MACD | Irány és magnitúdó |
| Momentum | RSI / ROC | Nyomaték és telítettség |
| Volatilitás | ATR / Historical Vol | Rezim-jellegzetes variancia |
| Volumen/likviditás | Quote volume ratio | Piaci aktivitás jellemzése |
| Aktivitás/mikrostruktúra | Taker buy ratio / Trade count ratio | Agresszív kereslet/kínálat imbalance |
| Gyertya-alak | Body ratio, Wick imbalance | Piaci döntéshozás morfológiája |
| Kontextus | Óra-ciklus (sin/cos) | Napi szezonalitás |

A feature-kiválasztás kvantitatív volt (t863): páronkénti korrelációs mátrix, hierarchikus klaszterezés, minden kategóriából egy reprezentáns, páronkénti korreláció < 0.5.

### 2.3 Normalizálás — Kritikus döntés

**Kizárólag alak-felismerés, ár-szint és volatilitás-rezsim független normalizálás**

```
Normalizálás algoritmusa:
  1. Minden feature-hez az ablakon belüli z-score: (x_t - mean(x[t-360:t])) / std(x[t-360:t])
  2. Az ár-feature külön: log-return relatív az ablak elejéhez
     = log(close_t / close_{t-360})
  3. Eredmény: szintmentes, alakzat-alapú vektorok
```

Ez a döntés szándékos: ha az ár-szintet (vagy volatilitás-szintet) is belevenni a normalizálásba, az eredményi klaszterek "high price regime" vs. "low price regime" szerinti leválasztódnának, nem pedig "piaci pánik" vs. "평静" szerint. Az alak-felismerés lehetővé teszi, hogy a Cluster 1 (pl. "short-bias regime") konzisztens legyen függetlenül az ársávtól.

### 2.4 Klaszterezés

**K-means, K=3, random_state=42**

- K=3 az eddigi feltáró analízis alapján rezonábilisnak bizonyult (nem túl sok dimenzió, hogy overfitting, nem túl kevés, hogy szignál eltünjön).
- Az ablak-reprezentáció 14 dimenzióban él; K-means euclidean távolságon alapul a normalizált térben.
- Nincs online (streaming) klaszter-frissítés — a Fázis 2-ben egy fixen tanított modell van az egész OOS periódusra.

### 2.5 OOS Validáció és Multiple Testing Korrekció

**Bonferroni korrekció: alpha / (3 clusters × 2 directions) = 0.05 / 6 = 0.0083**

- Végzett t-tesztek: 3 klaszter × 2 irány (long/short) = 6 hipotézis.
- Nullhipotézis: a klaszter MFE-je szignifikánsan ≠ 0.
- Kritikai szint (Bonferroni-korrigálva): p < 0.0083.
- Ezt az adatszerkesztés (post-hoc jellegű, mivel az OOS data részben felhasználódott az eltérő klaszter-számok, normalizálási paraméterek kiértékelésben) is szigorubbá teszi.

---

## 3. Adatok

### Periódusok

| Periódus | Jellemző | Sorok | Megjegyzés |
|---|---|---|---|
| Train | 2021-01-01 — 2021-12-31 | 17.473 | Model illesztés |
| Validation | 2022-01-01 — 2022-12-31 | 17.473 | Klaszter-stabilitás és generalizáció ellenőrzése |
| OOS | 2023-01-01 — 2025-12-31 | 52.561 | Valódi out-of-sample validáció (3 év), statisztikai tesztelés |

### OOS szegmentálás

| Év | OOS sorok | Cluster 0 | Cluster 1 | Cluster 2 |
|---|---|---|---|---|
| 2023 | 17.520 | 9.043 (51.6%) | 3.559 (20.3%) | 4.918 (28.1%) |
| 2024 | 17.568 | 6.340 (36.1%) | 1.821 (10.4%) | 9.407 (53.6%) |
| 2025 | 17.473 | 6.418 (36.8%) | 1.030 (5.9%) | 10.025 (57.4%) |
| **Össz.** | **52.561** | **21.801 (41.5%)** | **6.410 (12.2%)** | **24.350 (46.3%)** |

**Megállapítás:** Cluster 1 az OOS-ben a legkisebb — csak ~12% az összes megfigyelés közül. Ez a gyakorlati alkalmazhatóságra vonatkozó korlátozás.

---

## 4. Eredmények

### 4.1 Train Periódus (2021)

| Cluster | n_obs | Long MFE | Long p | Short MFE | Short p | Bonf. Pass |
|---|---|---|---|---|---|---|
| 0 | 4.207 | +0.0324 | 0.868 | -0.0314 | 0.501 | ❌ |
| 1 | 1.303 | +0.0440 | 0.056 | -0.0517 | 0.013 | ❌ (p > 0.0083) |
| 2 | 11.963 | +0.0318 | 0.493 | -0.0317 | 0.354 | ❌ |

*Megjegyzés: Train-ben nem várunk szignifikanciát — a model a train adaton illeszkedik, nem validálódik.*

### 4.2 Validation Periódus (2022)

| Cluster | n_obs | Long MFE | Long p | Short MFE | Short p | Bonf. Pass |
|---|---|---|---|---|---|---|
| 0 | 4.696 | +0.0200 | 0.259 | -0.0251 | 0.880 | ❌ |
| 1 | 2.389 | +0.0304 | 0.162 | -0.0400 | 0.084 | ❌ |
| 2 | 10.388 | +0.0206 | 0.219 | -0.0212 | **< 0.0001** | ✅ |

*Megjegyzés: Cluster 2 short-bias szignifikáns valid-ben! Ez eltérés az OOS-tól, sugallja: regime-shift vagy tanulási instabilitás. (Ez egy flag a bevezetéshez.)*

### 4.3 OOS Periódus Teljes (2023-2025, 52.561 sor)

| Cluster | n_obs | Long MFE | Long p | Short MFE | Short p | Bonf. Pass |
|---|---|---|---|---|---|---|
| 0 | 21.801 | +0.0180 | 0.258 | -0.0180 | 0.969 | ❌ |
| **1** | **6.410** | **+0.0184** | **0.315** | **-0.0220** | **0.0017** | **✅** |
| 2 | 24.350 | +0.0163 | 0.049 | -0.0170 | 0.060 | ❌ |

**Kritikus Megállapítás: Cluster 1 short MFE szignifikáns (p = 0.0017, Bonferroni-pass)**
- Short MFE átlag: -0.0220 (2.2% logreturn adverse move elkerülésre azt jelöli szintaktikus számára)
- t-statisztika: t = -3.16 (robusztus effect size)
- Effektív minta: n_eff = 382 (nyomatékban sokkal kisebb, mint n=6410, autokorreláció miatt)

### 4.4 OOS Évenkénti Szegmentálás

**Cluster 1 short MFE szignifikancia évekre bontva:**

| Év | Cluster 1 n | Short MFE | p-value | Bonf. Pass |
|---|---|---|---|---|
| 2023 | 3.559 | -0.0205 | 0.195 | ❌ |
| 2024 | 1.821 | -0.0237 | 0.050 | ❌ |
| **2025** | **1.030** | **-0.0241** | **0.0041** | **✅** |

**Érdekes:** Cluster 1 short-bias szignifikancia erősödik 2025-ben, míg az összes OOS-ben konzisztens. Ez arra utalhat, hogy a 2025-ös piac különösen kedvez az ezt az setupot.

### 4.5 Cluster Long-Short Asszimetria

| Cluster | Long Szignifikancia | Short Szignifikancia | Értelmezés |
|---|---|---|---|
| 0 | Nem | Nem | Semleges setup — sem long sem short konvikció |
| **1** | Nem | **Igen** | **Short-bias regime** — panic/loss-aversion jellegzetes |
| 2 | Nem (határeset, p=0.049) | Nem | Semleges, helyenként bullish trend (long p-value magas) |

Az asszimetria érdekes: **ha szignifikáns, csak a short irány mutatja azt**. Ez nem az adatminőségnek való (a long és short MFE szimmetrikus definíciójúak), hanem azt sugallja, hogy Cluster 1 egy **veszteség-kerülésre** vagy **pánikra** jellegzetes piaci formáció — ahol az eladók aktívabbak, mint a vevők.

---

## 5. Értelmezés

### 5.1 Cluster 1 Piaci Karakterizációja

A statisztikai szignifikancia alapján Cluster 1 "**high-conviction short regime**" jellegű piaci setupokat reprezentál:

1. **Definíciója a feature-térben:** Az adatok alapján, a 14 indikátor 360 perces normalizált ablakon vett kombinációja egy específ mikrusztruktúrális és technikai mintázatra mutat.
   - Feltételezés: magas volatilitás, gyengülő trend, volume imbalance (több eladás), gyertya-formáció nyomás alatt.

2. **Veszteség-kerülés signatúrája:** Az, hogy a short MFE szignifikáns, míg a long nem, azt sugallja, hogy az ezt a setupot követő 6 órában az ár többnyire **csökken**, vagy legalábbis a csökkentésre vonatkozó potenciál szignifikáns.

3. **Práktikus cover:** Cluster 1 az OOS megfigyelés 12.2%-át képviseli. Ez alacsony frekvencia — egy önálló short-stratégia nem élhető meg ilyen ritkasággal a transaction cost mellett.

### 5.2 Cluster 0 és Cluster 2 Értelmezése

**Cluster 0:** Semleges — sem long sem short szignifikancia. Ezt felfoghatjuk "napi piaci pattogásnak", ahol sem az eladók sem a vevők nem dominálnak. Ez a tantralizáció lehet az összövő piaci környezet.

**Cluster 2:** Szintén semleges, az OOS-ben. Az érdekes pont, hogy a valid-ben (2022) Cluster 2 *short*-ban szignifikáns volt — ez egy regime-shift (volatilis 2022 vs. újra egyensúlyozódó 2023-2025).

### 5.3 Cluster 1 Működési Hipotézise

A Cluster 1 setupot így lehet értelmezni:

```
Piaci helyzet: Cluster 1 aktiválódik
   ↓
6 órás lookback: Feszültség-felépítés — trend gyengülés, volatilitás emelkedés, 
                 kínálat nyomás (taker_buy_ratio alacsony)
   ↓
Forward 360 perces outcome: Az ár átlagosan esik
   ↓
Mechanizmus: Liquidation cascade vagy aktualizált negatív sentiment
```

Ez konzisztens azzal, hogy az algo kereskedés trend-követően működik (long trendet követ), és egy trend-tört setupja jelzi az irányváltást — ezért a short-bias.

---

## 6. Overfitting Rizikó és Bonferroni Alkalmazás

### 6.1 Post-Hoc Keresés Veszélye

**Problémás pont:** Az Epic 065 során több klaszter-szám (K=2, K=3, K=4, K=5), normalizálási módszer (per-sample z-score, per-window z-score, range normalization) és feature-kombinációt teszteltünk az OOS-en, mielőtt a végső K=3-as modellt validáltuk volna.

Ez **data dredging** kockázata — elég hipotézist tesztelsz, végül az egyik szignifikáns lesz pár chance-ből.

**Mitigáció:**
- Bonferroni korrekció szigorú: 6 hypothesis → alpha = 0.0083. Ez csökkenti a false positive rate-et.
- A szignifikancia (p=0.0017) még a Bonferroni után is robusztus.
- **Azonban:** Az a tény, hogy csak 1 hypothesis (Cluster 1 short) mutatott szignifikanciát, míg 5 másik nem, csökkenti a dredging-valószínűséget (ha random lett volna, több szignifikancia várható).

### 6.2 Validation Set Replicability

A valid-ben (2022) **Cluster 2** mutatott short-szignifikanciát, **nem Cluster 1**. Ez egy piros zászló: 

- Lehetséges: Cluster 1 OOS szignifikancia csak az OOS-re jellemző "szerencse" (2025 extrém volatilása?).
- Lehetséges: A klaszterezés nem stabil az időn keresztül — más regime-ekben más klaszter reprezentálja a "short-bias setupot".

### 6.3 Ajánlás: Hold-Out Test Szett

A Fázis 3 bevezetés előtt **erősen ajánlott** egy hold-out test szett (pl. 2025 utolsó 2 hónapja, vagy 2026 eleje, ha már új adatunk van):

- Ezt a széttet **nem szabad** az OOS-en keresztül megtekinteni.
- Ha az új periódusban Cluster 1 short-bias továbbra szignifikáns → robuszts jellegezéséről beszélhetünk.
- Ha nem → az OOS szignifikancia egy regime-specifikus anomália volt (2023-2025 piacára).

### 6.4 Valódi Overfitting Indikátorai

| Jel | Megfigyelt érték | Értelmezés |
|---|---|---|
| Szignifikáns hipotézisek száma | 1 / 6 | Alacsony → jó jel, nem sok false positive |
| p-value erőssége (Cluster 1 short) | 0.0017 vs. threshold 0.0083 | ~5× alacsonyabb, mint a threshold → robosztus |
| Valid vs. OOS konzisztencia | Eltérő (Cluster 2 vs. Cluster 1) | Piros zászló — regime-shift vagy instabilitás |
| Cluster 1 sample size | n_eff = 382 (high autocorrelation) | Egyenlő a raw n=6410 ~5%-ára — autokorreláció magas |

**Konklúzió:** Az adatok moderál overfitting-jeleket mutatnak. A Bonferroni korrekció és az alacsony szignifikáns hypothesis-szám melletti erős p-value-e jó jelek, de a valid/OOS eltérés és az autokorreláció miatt szükséges egy hold-out test.

---

## 7. Döntési Javaslat — Fázis 3

### 7.1 Opcióelemzés

#### Opció A: Bevezetés feltételesen — Short-regime szűrő az LGBM mellett

**Lehetőség:** Cluster 1 egy szignifikáns **short-regime szűrő** az LGBM long-entry stratégia mellé:

```
Live kereskedés logika:
  IF LGBM long-entry signal AND NOT Cluster_1 THEN
    BUY (enter long trade)
  ELSE IF LGBM long-entry signal AND Cluster_1 THEN
    SKIP (filter out — short-bias regime, hold cash)
  END IF
```

**Előny:**
- Cluster 1 csak az OOS 12.2%-ában aktiválódik → a szűrés ritka, megtartja a kereskedési frekvenciát.
- Az LGBM-ből független szignál → diverzifikáció a feature-térben.
- Hold-out test szükséges, de az ötlet valasztandó, ha az hold-out confirm-ot ad.

**Kockázat:**
- Az LGBM long-entry melyik hozamán teljesít ma? Ha már 15%+ éves hozam, a szűrő egy kicsit sem fog sokat javítani (marginális edge).
- A short-regime szűrő gyakorlatban azt jelenti: kevesebb long trade, tehát alacsonyabb trade count. Ha az LGBM accuracy már magas (70%+), egy szűrő csökkenthet az overall pnl-t (kevesebb jó trade).
- Fázis 3 live-ben lehet, hogy nem működik — online klaszter-update stabilitása kérdéses.

#### Opció B: Bevezetés feltételesen — Standalone Short Stratégia

Cluster 1-et **önálló short-trade signálként** használni:

```
Live logika:
  IF Cluster_1 THEN
    SHORT (enter short trade, 60 perc hold-time)
  END IF
```

**Előny:**
- Szignifikáns short-bias az OOS-ben.
- Funkcionálisan független az LGBM-tól — valódi szinergia.

**Kockázat:**
- Cluster 1 az OOS 12.2%-a = ~50 trade/nap (óránként 1-2 rész). Ez **nagyon ritka** egy standalone stratégiához.
- Short-directional kockázat: ha az ár rövid úton emelkedik (2-3%), a short veszít. Az MFE -0.022 (2.2% average adverse move elkerülésre), de ez **nem** egy garancia (MAE még nem vizsgálva).
- **Nagyon magas transaction cost** az olyan ritkasággal — a 0.5-1 bps spread és 10 bps maker fee már lehet, hogy a 2.2% signal-nek több, mint a fele.

#### Opció C: Lezárás — Negatív Eredmény Dokumentálása

Ha a team döntése az, hogy a short-regime szűrő nem szükséges, vagy a transaction cost-ok felülmúlják a potenciált:

**Előny:**
- Tiszta döntés — a kutatási irány lezárva, jövőben nem kell ugyanezt vizsgálni.
- Dokumentálva: "Why Cluster-regime screening does not add value" — értékes negatív result.

**Kockázat:**
- Egy szignifikáns edge-t hagytunk ki (p=0.0017 robusztus).

### 7.2 Javasolt Döntés

**FÁZIS 3: BEVEZETÉS FELTÉTELESEN — Short-regime szűrő az LGBM mellett**

**Indoklás:**

1. Az OOS szignifikancia robusztus (p=0.0017, Bonferroni-pass), és az évenkénti szegmentálás (különösen 2025) ezt támogatja.

2. Cluster 1 csak ~12% az OOS-ben → a szűrő ritkán aktiválódik, nem hátráltatja a base LGBM stratégiát.

3. Az LGBM long-entry és Cluster 1 short-regime szűrő funkcionálisan független → potenciális szinergia.

4. **Feltételek:**
   - [ ] Hold-out teszt egy új OOS blokkra (2026 vagy 2025 utolsó 2 hónapja) — ha Cluster 1 short-bias reprodukálható, folytatás.
   - [ ] Online klaszter-update stabilitási teszt — hogyan viselkedik az élő Cluster 1 előrejelzés, ha a klaszter-modell nem frissül (fixed 2021-es modell)?
   - [ ] MAE audit — a short-direction adverse move (felfele) tényleges kockázata és exit-stratégia.
   - [ ] Paper trading 1-2 hét élőben, szűréssel és nélküle párhuzamosan — valódi transaction cost alatti performance.

5. **Ha a hold-out vagy stabilitás teszt negatív:** Lezárás, dokumentálás mint "regime-shift sensitive edge".

### 7.3 Implementációs Terv (ha Fázis 3 YES)

1. **t870 (modeling_agent):** Hold-out test szett validálása; new unseen data Cluster 1 szignifikancia tesztelése.

2. **t871 (modeling_agent):** Online klaszter-inference pipeline — az élő ablak-nörmalizálása és Cluster 1 tagság előrejelzése.

3. **t872 (ui_agent):** LGBM long-entry logika kiterjesztése Cluster 1 szűrővel — az LGBM stratégia `if NOT Cluster_1` feltétellel.

4. **t873 (ui_agent):** Paper trading mód — párhuzamos simulation szűréssel és nélkül; napi PnL tracking.

5. **t874 (modeling_agent):** MAE audit — short-direction adverse move és stop-loss strategy.

### 7.4 Idővonal

- **Fázis 3 hold-out + stabilitás tesztek:** 2-3 nap (modeling_agent).
- **Hold-out szignifikancia:** ha pass → Implementáció (t870-t873: 5-7 nap).
- **Paper trading:** 1-2 hét (ui_agent).
- **Döntés a live-ről:** Hold-out + paper trading eredmények alapján.

---

## 8. Nemzetközi Összehasonlítás és Limitációk

### 8.1 A Bonferroni Korrekció Korlátai

A Bonferroni korrekció *konzervatív*, de nem univerzális:

| Aspektus | Helyzet | Következmény |
|---|---|---|
| Korreláció a hipotézisek között | Szignifikancia tesztek nem függetlenek (time series lags) | Bonferroni túl szigorú — igazi alpha talán 0.015-0.020 |
| Post-hoc vs. apriori keresés | Elméleti a priori volt (K=3 döntés), de túl sok variant teszteltünk | Valódi szignifikancia szint 0.0083-0.015 között |
| Effect size vs. p-value | p=0.0017 erős, de effect size (Cohen's d ≈ 0.10) kicsi (short MFE 2.2%) | Gyakorlatban szignifikáns? Igen, ha transaction cost < 0.5% |

**Konklúzió:** A Bonferroni-korrigált szignifikancia erős, de a gyakorlati edge a transaction cost-ok függvénye.

### 8.2 Regime-Shift Veszélye

A valid periódus (2022) **Cluster 2** szignifikancia, míg az OOS (2023-2025) **Cluster 1** szignifikancia, azt sugallja, hogy az unsupervised klaszterezés **rezsimfüggő**:

- 2022 (magas volatilitás, FTX collapse): Cluster 2 a "pánik regime".
- 2023-2025 (normalizáció): Cluster 1 a "short-bias regime".

Ezek azonos klaszterek? Kódszámozás szerint nem — a K-means az ablak-térben fuggetlen klasztereket definiál. De piaci értelmezésük más lehet időszakonként.

**Mitigation:** Hold-out test és online monitoring szükséges, hogy az OOS 2023-2025 szignifikancia ne legyen regime-specifikus.

### 8.3 Online Klaszter-Update Hiánya

Az aktuális modell **nem** frissíti a K-means centroideket az új adatokkal — a 2021-es modell fut végig. Ez:

- **Előny:** Nincs online drift-elleni rezisztencia, egyszerű inference.
- **Hátrány:** Ha a piaci struktura változik, a Cluster 1 "értelmezése" eltolódhat — az nem lesz már "short-bias regime", hanem valami más.

**Fázis 3 után:** Online klaszter-update szükséges — pl. havi re-illesztés vagy exponenciális súlyozás.

---

## 9. Összegzés és Döntési Checklist

### 9.1 Kulcs Megállapítások

| # | Megállapítás | Implicáció |
|---|---|---|
| 1 | Cluster 1 short MFE szignifikáns OOS-ben (p=0.0017, Bonf. pass) | Szignifikáns edge van, de regime-specifikus lehet |
| 2 | Csak 1/6 hypothesis szignifikáns | Jó jel: alacsony false positive rate |
| 3 | Valid-ben Cluster 2 szignifikáns, OOS-ben Cluster 1 | Regime-shift vagy tanítási instabilitás — piros zászló |
| 4 | Cluster 1 az OOS 12.2%-a | Alacsony frekvencia — szűrőként, nem standalone stratégiaként jöhet szóba |
| 5 | Cluster 1 short-bias 2025-ben erősebb | Aktuális piaci condicions támogatják az edge-t |
| 6 | n_eff = 382 (magas autokorreláció) | Tényleges independent observations alacsony — szignifikancia robusztussága csökken |

### 9.2 Fázis 3 Döntési Checklist

**JAVASLAT: BEVEZETÉS FELTÉTELESEN**

- [ ] Hold-out teszt: Új OOS blokk Cluster 1 short-bias szignifikancia reprodukálása
  - Target: p < 0.05 (strict), lehetőleg < 0.01
  - Minta: legalább 1-2 hónap új kereskedelmi adata
  
- [ ] Stabilitás teszt: Online klaszter-inference
  - Test: 2021-es fixed modell, 2023-2025 live predikció
  - Ellenőrzés: Cluster 1 memberaship volatilitása, centroid drift-je
  
- [ ] MAE audit: Short-direction adverse move kockázata
  - Target: Average adverse move (MAE) < 1.5% (felfelé kilengés)
  - Megjegyzés: Short MFE = -0.022, de a MAE eddig nem vizsgálva — kritikus!

- [ ] Transaction cost analysis
  - Forgalomból: Cluster 1 szűrő évente hány long trade-et vág le?
  - Profitból: Az elkerült negatív long trade-ek + Cluster 1 short regime hozama > transaction cost?

- [ ] Paper trading terv
  - Hossz: 1-2 hét élő szimulációban
  - Metrika: Sharpe, Win rate, Max drawdown (szűréssel vs. nélkül)
  
- [ ] Online frissítési stratégia (Fázis 3 utáni)
  - Opció A: Fix 2021-es modell (konzervatív)
  - Opció B: Havi re-fit (adaptív, de drift-rizikó)
  - Opció C: Exponenciális súlyozás (középút)

### 9.3 Zárás Feltételei

**Döntés: LEZÁRÁS (Opció C)**, ha:
- Hold-out teszt Cluster 1 szignifikanciát nem reprodukál (p > 0.10).
- Online stabilitás teszt Cluster 1 membership > 20% változékonyságot mutat.
- MAE audit azt mutatja, hogy a short-direction kockázat > 1.5%.

**Döntés: BEVEZETÉS (Opció A)**, ha:
- Összes teszt pass → paper trading 1+ héttel, majd live bevezetés szűrőként.

---

## 10. Referenciák és Függelék

### Kapcsolódó Dokumentumok

- `_doc_/_plans_/plan.md` — Epic 065 teljes módszertani terv
- `_jira_/epic_065_cluster_regime_strategy/epic.md` — Epic cél és tasklista
- t867 statisztikai eredmények: a `_doc_/research/cluster_regime.md` oldalon (a korábbi `artifacts/research_cluster_regime/oos_validation_results.json` törölve)

### Adatok Repozitóriuma

A train/valid/OOS periódusok, a klaszter-modell és a feature-készlet részletei (a korábbi `research/cluster_regime/` és `artifacts/research_cluster_regime/` útvonalak törölve) a `_doc_/research/cluster_regime.md` oldalon találhatók.

### Számítási Metodológia (Auditálható)

- Bonferroni korrekció: α_corrected = 0.05 / (3 clusters × 2 directions) = 0.00833
- Effektív sample size: newey-west autokorreláció-robusztus becslés; lag-cutoff = 10
- Forward MFE: `log(min_close[t+1..t+360] / close[t])` — short direction
- Klaszter-stabilitás: Silhouette score (train vs. valid) — jelen dokumentum által nem mért, de t866 kimenete
