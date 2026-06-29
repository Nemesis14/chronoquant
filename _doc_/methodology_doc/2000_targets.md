# 2000 — Target Layer

## Overview

A target layer a ChronoQuant ML pipeline label-rétege. A target tábla az `ohlcv` nyers árból számított, jövőbe tekintő (forward-looking) outcome oszlopokat tartalmaz — ezek alkotják a modellek tanítási célváltozóit.

```mermaid
flowchart TD
    OHLCV["ohlcv tábla\nopen_time, close, high, low"]
    TARGET["target tábla\nforward outcome oszlopok"]
    SAMPLE["Sampling modul\ntrain/valid split"]
    TRAIN["LightGBM tanítás\ntarget col = long_mfe_fw60\nvagy short_mfe_fw60"]
    EVAL["Evaluation\nbacktest, OOS validáció"]

    OHLCV -->|Forward window számítás| TARGET
    TARGET --> SAMPLE
    SAMPLE --> TRAIN
    TARGET --> EVAL
```

**Aktív target oszlopok:** `long_mfe_fw60`, `short_mfe_fw60` — 60 perces forward logreturn outcome-ok.

A target layer dönti el, hogy a modell **mit tanul meg előrejelezni**. A helyes target definíció kritikus: ha torz, a validációs score nem tükrözi a valós produkciós teljesítményt.

---

## A Forward MFE Target

### Definíció

**MFE (Maximum Favorable Excursion):** az a maximális nyereség, amelyet egy pozíció elméletileg elért volna a tartási horizonton belül — anélkül, hogy bármilyen exit döntést hoznánk.

```mermaid
graph LR
    subgraph LONG["Long pozíció t-től"]
        LM["long_mfe_fw60\n= log(max_close[t+1..t+60] / close[t])\n→ pozitív ha ár felmegy"]
        LA["long_mae_fw60\n= log(min_close[t+1..t+60] / close[t])\n→ negatív ha ár lemegy\naudit célra"]
    end
    subgraph SHORT["Short pozíció t-től"]
        SM["short_mfe_fw60\n= log(min_close[t+1..t+60] / close[t])\n→ negatív ha ár esik\n(short kedvező)"]
        SA["short_mae_fw60\n= log(max_close[t+1..t+60] / close[t])\n→ pozitív ha ár emelkedik\n(short ellen)"]
    end
```

A modell `long_mfe_fw60` targetre tanul long irányban, `short_mfe_fw60` targetre short irányban. A MAE értékek nem elsődleges targetok, de az evaluation és adverse move audit során kötelezően ellenőrizendők.

### Forward window szemantika

```mermaid
flowchart LR
    T["t bar\n(aktuális, zárt gyertya)"] -->|"KIZÁRVA\nt nem része\na forward ablaknak"| FW["forward window\nt+1 .. t+60\n60 bar"]
    FW -->|max| MAX["fw60_max\nlong MFE alapja"]
    FW -->|min| MIN["fw60_min\nshort MFE alapja"]
    FW -->|t+60-as close| CL["fw60_close\nauxiliary"]
```

Az aktuális bar (`t`) kizárása kötelező: a predikció a `t` bar zárásakor készül, és a `t+1` bar nyitásán kerül végrehajtásra. Ha `t` benne lenne a forward ablakban, az outcome egy részben már ismert értéket tükrözne.

**NULL tail:** Az utolsó 60 sor minden outcome oszlopban `NULL` — nincs 60 jövőbeli bar. Ezeket a sorokat soha ne töltsd fel `0`-ra: a null nem negatív esemény, hanem ismeretlen kimenet.

---

## Üzleti és módszertani háttér

### Miért kritikus ez a lépés?

Ha a target definíció torz, az egész downstream pipeline félrevezető — a modell olyasmit tanul meg előrejelezni, ami nem felel meg a valós kereskedési célnak, vagy ami adatminőségi problémát hordoz magában.

```mermaid
flowchart TD
    subgraph PROB["Target definíciós hibák"]
        P1["Target-definition leakage\nJövőbeli eloszlásból vett küszöb\nbelekerül a múltbeli labelbe"]
        P2["Rezsimfüggő torzítás\nVolatilis időszak megváltoztatja\naz összes historikus label értékét"]
        P3["Információvesztés\nBináris label elveszíti a\nprofit-potenciál intenzitását"]
    end
    subgraph EFFECT["Következmény"]
        E1["Torzult validációs score"]
        E2["Félrevezető feature importance"]
        E3["Nem production-like threshold"]
    end
    PROB --> EFFECT
```

### Miért MFE és nem realized return?

Az MFE a **maximális potenciált** méri, nem a TP/SL döntést. Ez szándékos szétválasztás:

| Kérdés | MFE alapú target | Realized return alapú target |
|---|---|---|
| Mit mér? | Legjobb elérhető outcome | Tényleges végrehajtott hozam |
| TP/SL függő? | Nem — TP/SL nincs benne | Igen — TP/SL megváltoztatja |
| Modell feladata | Jó lehetőséget rangsorolni | Konkrét exit rule-t tanulni |
| Újraoptimalizálás | TP/SL keresés offline | Modell újratanítás kell |

A rendszer logikájában a modell feladata a lehetőségek rangsorolása. A **strategy réteg** (→ `6000_strategy.md`) dönti el, hogy a jó lehetőségből mikor és hogyan lép be/ki a live kereskedés. Ez a szétválasztás teszi lehetővé, hogy a TP/SL paramétereket a modell újratanítása nélkül optimalizáljuk.

### Miért 60 bar (60 perc)?

```mermaid
flowchart LR
    FW60["60 perces forward window"]
    LIVE["Live rendszer\nmax holdingidő: 60 perc"]
    CALIB["Strategy calibration\nkalibrációs ablak"]
    GRID["Grid search\nmax_hold_bars = 60"]

    FW60 --> LIVE
    FW60 --> CALIB
    FW60 --> GRID
```

A 60 bar horizon egységes a teljes pipeline-on: a live rendszer is 60 perc után zár timeout-tal, a grid search is 60 bar-t szimulál. Ez a konzisztencia biztosítja, hogy a modell ugyanolyan időhorizonon optimalizál, amelyen a live kereskedés fut.

### Miért folytonos regresszió és nem bináris osztályozás?

```mermaid
flowchart LR
    subgraph BINARY["Bináris label — Elvetett"]
        B1["future_max >= quantile threshold\n→ 0 vagy 1"]
        B2["Target-definition leakage:\nküszöb teljes historyból számolódik"]
        B3["Információvesztés:\n0.3% és 4.5% MFE\nmindkettő = 1"]
        B4["Rezsimfüggő torzítás:\nmagas vol = magasabb küszöb\n= kevesebb historikus 1"]
    end
    subgraph CONT["Folytonos target — Választott"]
        C1["log(max_forward / close)\nvalós szám"]
        C2["Nincs küszöb-torzítás"]
        C3["Magnitude megmarad"]
        C4["Rezsimfüggetlen definíció"]
    end
```

| Megközelítés | Előny | Hátrány | Státusz |
|---|---|---|---|
| Folytonos fw60 logreturn (jelenlegi) | Nincs percentilis-torzítás, magnitude megmarad, flexibilis | Regresszor szükséges, binary baseline elvész | ✅ Választott |
| Full-history quantile bináris | Egyszerű classifier, stabil threshold | Target-definition leakage, rezsimfüggő torzítás, információvesztés | ❌ Eltávolítva — legacy |
| Fold-specifikus quantile bináris | Leakage-mentes binarizálás | Minden foldban különböző label → összehasonlíthatatlan metrikák | ⚠️ Derived label-ként fontolóra vehető |
| Triple-barrier | MFE + MAE egyszerre kezel, stop-loss implicit | Konfiguráció érzékeny, training instabilabb | ⚠️ Jövőbeli kísérlethez |

### Miért logreturn és nem simple return?

| Mérőszám | Jellemző | Alkalmazhatóság |
|---|---|---|
| Simple return `(max − close) / close` | Aszimmetrikus: +10% és −10% nem összehasonlítható | Napi riporthoz elfogadható |
| Logreturn `log(max / close)` | Additív, szimmetrikus; kis értékeknél ≈ simple return | ML target, multi-period aggregáció |

A logreturn additív természete lehetővé teszi, hogy a long és short MFE értékek közvetlenül összehasonlíthatók legyenek, és multi-period kilátások összeadással aggregálhatók. Kis moves esetén (<2%) a numerikus különbség elhanyagolható.

### Paraméter alapértékek és indoklásuk

| Paraméter | Érték | Indoklás |
|---|---|---|
| Forward horizon | `60` bar | 60 perces opportunity ablak; egyezik a live max holdingidővel |
| Window logika | `t+1..t+60` | Aktuális bar kizárva; pontosan 60 jövőbeli bar |
| NULL küszöb | `fw_bar_count >= 60` | Csak teljes forward ablakkal rendelkező sorok kapnak értéket |
| Logreturn alap | természetes logaritmus (LN) | Szimmetrikus, additív |
| Elsődleges long target | `long_mfe_fw60` | MFE = maximális long opportunity |
| Elsődleges short target | `short_mfe_fw60` | MFE short oldalon = log(min/close) — legkedvezőbb short |
| Rebuild policy | teljes DELETE+INSERT | Minden sync hívás teljes újraszámítást végez — idempotens |

### Ismert kockázatok és korlátok

| Kockázat | Tünet | Mitigáció |
|---|---|---|
| NULL tail torzítás | Ha sampling elszedi az utolsó 60 sort és 0-ra imputálja | Null target sorok droppolva a dataset loaderben; soha ne impute-old 0-ra |
| Rezsimváltás eltérő target eloszlást okoz | Alacsony volatilitásban az MFE p90 kisebb | Expanding window kontextus; a valid periódus követi a legfrissebb rezsimet |
| `short_mfe_fw60` szemantikai félreértés | Pozitív short_mfe értéket hibásnak vélni | Szabály: short_mfe < 0 mindig igaz profitábilis short esetén — ez nem adathiba |
| Kis logvalue értelmezése | `long_mfe_fw60 = 0.003` → ~0.30% mozgás — konfúzió a magnitude körül | Riportokban mindig % formában is feltüntetni |
| Legacy target referencia | Régi bináris `trg_*` targetekre hivatkozó dokumentáció | Elavultként kezelni; ground truth: ez a specifikáció és a forráskód |

### Validációs checklist

- [ ] A target tábla utolsó 60 sora minden fw60 outcome oszlopban `NULL`
- [ ] Az aktuális bar (`t`) nem szerepel a forward ablakban — csak `t+1..t+60`
- [ ] `long_mfe_fw60` = `log(fw60_max / close)` — numerikusan ellenőrzött determinisztikus teszttel
- [ ] `short_mfe_fw60` = `log(fw60_min / close)` — numerikusan ellenőrzött determinisztikus teszttel
- [ ] `long_mfe_fw60` és `short_mfe_fw60` csak DOUBLE `NULL`, soha `0.0` — nem impute-olt
- [ ] A target tábla nem tartalmaz legacy `trg_*` bináris oszlopot
- [ ] Sync után: computed_from, computed_to, computed_at metaadat frissítve
- [ ] Dataset loader: null target sorok droppolva — nincs `0` imputation
