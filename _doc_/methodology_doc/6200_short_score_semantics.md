# 6200 — Short Score Szemantika

## Overview

A short modell score-ja, percentilis rangsorolása és belépési logikája ellentétes intuíciót követ a long oldallal szemben. Ez a dokumentum az inverziós logika teljes módszertani indoklását adja — a raw targettől a belépési döntésig.

```mermaid
flowchart TD
    RAW_L["pred_long_raw\nnyers long score\nmagas = erős long lehetőség"]
    RAW_S["pred_short_raw\nnyers short score\nalacsony = erős short lehetőség"]

    PCT_L["score_pct_long\nmagas (0.97) = top 3% long szignál"]
    PCT_S["score_pct_short\nalacsony (0.03) = top 3% short szignál"]

    ENTRY_L["score_pct_long >= cutoff_long\npl. >= 0.98\n→ top 2%"]
    ENTRY_S["(1 - score_pct_short) >= cutoff_short\npl. >= 0.94\n→ bottom 6%"]

    LONG_TRADE["ENTER_LONG"]
    SHORT_TRADE["ENTER_SHORT"]

    RAW_L --> PCT_L --> ENTRY_L --> LONG_TRADE
    RAW_S --> PCT_S --> ENTRY_S --> SHORT_TRADE
```

Az inverziós logika három rétegű. Ha bármelyik réteg összetévesztett, a live service rossz irányba kereskedik.

---

## A három inverziós réteg

### 1. réteg — Target definíció: miért negatív a short_mfe_fw60?

```mermaid
flowchart LR
    CLOSE["close = 100 (entry ár)"]

    subgraph WEAK["Gyenge short lehetőség"]
        MIN30["fw_min = 97\n(3% esés)"]
        MFE30["short_mfe_fw60\n= log(97/100)\n= -0.030"]
    end

    subgraph STRONG["Erős short lehetőség"]
        MIN60["fw_min = 94\n(6% esés)"]
        MFE60["short_mfe_fw60\n= log(94/100)\n= -0.062"]
    end

    CLOSE --> MIN30 --> MFE30
    CLOSE --> MIN60 --> MFE60
```

A short MFE (`log(fw_min / close)`) **mindig negatív**, ha az ár esett. Minél erősebben esett, annál nagyobb az abszolút értéke — és annál jobb volt a short lehetőség. Ez nem adathiba, hanem a log-return konvenció természetes következménye.

**Következmény a modellre:** A short modell alacsonyabb (negatívabb) nyers score-t ad a jobb short lehetőségekre. Ez megfordítja az intuíciót a long irányhoz képest, ahol a magasabb score jelenti a jobb lehetőséget.

### 2. réteg — Rank szemantika: miért alacsony score_pct_short a legjobb?

```mermaid
flowchart TD
    CALIB["Kalibrációs periódus\nscore_raw növekvő sorrendbe rendezve"]
    LOW_RAW["Alacsony pred_short_raw\n→ alacsony score_pct_short\npl. 0.03"]
    HIGH_RAW["Magas pred_short_raw\n→ magas score_pct_short\npl. 0.97"]
    GOOD_SHORT["Legjobb short szignál\n(erős esés várható\nnegatívabb short_mfe_fw60)"]
    WEAK_SHORT["Gyenge short szignál\n(kis vagy nincs esés)"]

    CALIB --> LOW_RAW --> GOOD_SHORT
    CALIB --> HIGH_RAW --> WEAK_SHORT
```

A rank lookup a kalibrációs periódusban a `pred_short_raw` **növekvő** sorrendjéhez rendel 0–1 értékeket. Mivel az alacsonyabb raw score jelzi a jobb short lehetőséget, az alacsony `score_pct_short` = a legjobb short szignál.

**Ez az inverz a long logikához képest:** long-nál magas `score_pct_long` = jobb long.

### 3. réteg — Entry feltétel: miért `(1 - score_pct_short) >= cutoff`?

```mermaid
flowchart LR
    subgraph LONG_COND["Long belépési logika"]
        LC["score_pct_long >= cutoff_long\nTop N% score kerül be"]
    end
    subgraph SHORT_COND["Short belépési logika — Invertált"]
        SC["(1 - score_pct_short) >= cutoff_short\nBottom N% score kerül be"]
        SC_EQ["Ekvivalens: score_pct_short <= (1 - cutoff)\npl. cutoff=0.94 → score_pct <= 0.06"]
    end
    subgraph UNIFIED["Egységes cutoff szemantika"]
        U["A cutoff mindkét irányban\na legerősebb szignál-sávot jelöli"]
    end

    LONG_COND --> UNIFIED
    SHORT_COND --> UNIFIED
```

Az `(1 - score_pct_short) >= cutoff_short` forma tartja fenn a szimmetriát: mindkét oldalon ugyanaz a `cutoff` paraméter jelenti a "legalább ilyen erős szignált engedünk be" korlátot. Ez az egységes cutoff szemantika teszi a két irány összehasonlíthatóvá.

**Konkrét példa:** `entry_cutoff_short = 0.94`
- Feltétel: `(1 - score_pct_short) >= 0.94`
- Ekvivalens: `score_pct_short <= 0.06`
- Eredmény: csak a bottom 6% score-ú barokra lép be

---

## Üzleti és módszertani háttér

### Miért kritikus ez a lépés?

A short score szemantikájának félreértése közvetlen trading hibát okoz:

```mermaid
flowchart TD
    ERR["Hiba: entry feltétel\nscore_pct_short >= cutoff\n(nem invertált)"]
    RESULT_ERR["Belép a legrosszabb short baroknál\n(ahol score_pct_short magas\n= gyenge short lehetőség)"]
    MISS["Kihagyja a legjobb short lehetőségeket\n(ahol score_pct_short alacsony)"]

    ERR --> RESULT_ERR --> MISS
```

### Miért nem invertáljuk a raw score-t közvetlenül?

Felmerülő kérdés: miért nem szorozzuk `-1`-gyel a `pred_short_raw`-t, és ezután long-analóg "magas = jó" logikával kezeljük?

| Megközelítés | Előny | Hátrány | Státusz |
|---|---|---|---|
| **Invertált percentilis `(1 - score_pct)`** | Konzisztens rank lookup artifact; isotonic is változatlan | Inverziós logikát explicit dokumentálni kell | ✅ Választott |
| Raw score `× -1` invertálás | Intuitívabb long-analóg kezelés | Külön rank lookup kell; isotonic fitting is változik | ❌ Elvetett |
| Különálló short modell ascending targettel | Intuitív | Eltérő training target = eltérő modell architektúra kell | ❌ Elvetett |

A percentilis rank lookup (`rank_lookup_short`) a `pred_short_raw` **növekvő** sorrendjéhez épül fel — ha a raw score-t invertálnánk, a lookup tábla inkompatibilissé válna. Az `(1 - score_pct_short)` inverziós forma megtartja az artifact konzisztenciát.

### Konzisztens alkalmazás a pipeline-ban

Az inverziós logika három helyen jelenik meg, és mindhárom helyen konzisztensen kell alkalmazni:

```mermaid
flowchart LR
    subgraph CALIB["Kalibrálás"]
        C1["rank lookup: növekvő raw score sorrendben\nalacsony raw → alacsony score_pct"]
    end
    subgraph SEARCH["Hyperparameter search"]
        S1["Objektív: valid_ratio_p075\nbottom 7.5% score → legjobb short MFE\n(mindkét szám negatív)"]
    end
    subgraph GRID["Grid search"]
        G1["Entry feltétel: (1 - score_pct) >= cutoff\nBottom N% szignál-sávból enged be"]
    end
    subgraph LIVE["Live runtime"]
        L1["strategy.py:\n(1.0 - score_pct_short) >= entry_cutoff_s"]
    end
    CALIB --> SEARCH --> GRID --> LIVE
```

### Paraméter alapértékek és indoklásuk

| Paraméter | Alapérték | Indoklás |
|---|---|---|
| `entry_cutoff_short` | `0.94` (top 6%) | Grid search optimum: 260 trade, 62.3% win rate |
| `entry_cutoff_long` | `0.98` (top 2%) | Külön long session optimuma; short cutoff-tól független |
| Rank lookup periódus | Kalibrációs periódus | Offline fit; live runtime változatlanul fogyasztja |
| `score_pct_short` clip | `[0.0, 1.0]` | Extrapoláció helyett clamping; live-ban is biztonságos |

### Ismert kockázatok és korlátok

| Kockázat | Tünet | Mitigáció |
|---|---|---|
| Dokumentációs félreértés az inverzióról | Tévesen `score_pct_short >= cutoff` kerül a kódba | Ezt a dokumentumot minden strategy-érintő code review-ban referenciálni kell |
| Kalibrációs periódus drift | A rank lookup elavul; `score_pct_short` nem tükrözi az aktuális rezsimet | Rendszeres re-kalibrálás, különösen rezsimváltás után |
| Long_priority konflit | Ha mindkét szignál tüzel, a long nyer — a shortot kizárhatja magas volatilitásban | Szándékos (strategy.py); dokumentált viselkedés |
| Isotonic és rank lookup inkonzisztencia | Ha a rank lookup újra fut, az isotonic is újraépítendő | `fit_calibration()` egyszerre futtatja mindkettőt |

### Validációs checklist

- [ ] `strategy.py` belépési feltétel: `(1.0 - score_pct_short) >= entry_cutoff_s` — nem `score_pct_short >= cutoff`
- [ ] `rank_lookup_short` a kalibrációs periódus `pred_short_raw` alapján épül (növekvő sorrend)
- [ ] `short_mfe_fw60` negatív értékei megfelelőek — nem adathiba
- [ ] A grid search dokumentációja az `entry_cutoff_short = 0.94` értéket rögzíti
- [ ] A live `trading.json` a helyes strategy session ID-re mutat
- [ ] A hyperparameter search short objective (`valid_ratio_p075`) konzisztens a bottom 7.5% logikával
- [ ] Minden strategy-érintő implementációs változásnál: ez a dokumentum referenciálva
