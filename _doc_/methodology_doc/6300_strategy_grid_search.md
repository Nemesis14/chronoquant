# 6300 — Execution-Aware Strategy Grid Search

## Overview

Az execution-aware grid search a kalibrált score-teret **realizált loghozam alapján** értékeli ki — nem proxy mutatókon, hanem explicit intrabar végrehajtási modellen mérve. A keresés azt optimalizálja, hogy a tényleges belépési és kilépési szabálykészlet mit termelne trade-szinten.

```mermaid
flowchart TD
    CAL["Kalibrált scored tábla\nscore_pct_long, score_pct_short\nbucket statisztikák"]
    GRID["Determinisztikus paraméterrács\nentry_cutoff × tp_spec × sl_spec\n~200 setup irányonként"]
    EXEC["Intrabar TP/SL szimuláció\nhigh/low touch — 60 bar horizon"]
    SCORE["Összesített fact_log_return\nirányonként, setup-onként"]
    BEST_L["Legjobb long setup\ncutoff=0.98"]
    BEST_S["Legjobb short setup\ncutoff=0.94"]
    ART_L["strat_solusdt_fw60_long_2101_2605\nLong strategy artifact"]
    ART_S["strat_solusdt_fw60_short_2101_2605\nShort strategy artifact"]

    CAL --> GRID --> EXEC --> SCORE
    SCORE --> BEST_L --> ART_L
    SCORE --> BEST_S --> ART_S
```

---

## Dual-Session Architektúra

### A jelenlegi két önálló session

| Session | Irány | Entry cutoff | Trade-szám | Win rate | Compounded |
|---------|-------|-------------|-----------|----------|------------|
| `strat_solusdt_fw60_long_2101_2605` | Long | 0.98 (top 2%) | 78 | 79.5% | +50.1% |
| `strat_solusdt_fw60_short_2101_2605` | Short | 0.94 (top 6%) | 260 | 62.3% | +22.7% |

### Miért két önálló session?

```mermaid
flowchart LR
    subgraph COMBINED["Kombinált session — Kivezetett"]
        C1["Egységes entry_cutoff\nLong + Short együtt"]
        C2["Long: cutoff túl alacsony\n→ gyenge belépések engedve"]
        C3["Short: cutoff feleslegesen szűk\n→ jó szignálok kihagyva"]
        C1 --> C2
        C1 --> C3
    end
    subgraph DUAL["Dual session — Aktív"]
        D1["Long session\ncutoff=0.98 (top 2%)"]
        D2["Short session\ncutoff=0.94 (top 6%)"]
    end
```

A long és short modell score-eloszlása eltér. A long side csak az extrém magas percentilnél ad megbízható szignált (top 2%), a short side viszont szélesebb sávban is jövedelmező (top 6%). Egyetlen shared `entry_cutoff` esetén az egyik irány szükségszerűen szuboptimálisan hangolt.

| Kritérium | Kombinált session | Dual session |
|---|---|---|
| Long cutoff optimalizálás | Kompromisszumos | Független (cutoff=0.98) |
| Short cutoff optimalizálás | Kompromisszumos | Független (cutoff=0.94) |
| Audit — melyik irány rontott? | Összemosott | Irányonként elkülönített |
| Re-optimalizálás (csak long) | Mindkét irányt érinti | Csak a long session cserélhető |

**Szabály:** Kombinált session shared cutoff-fal nem megengedett. Ha új periódusra kell re-optimalizálni, mindkét irányt külön kell lefuttatni és külön artifact-ba menteni.

---

## A Keresési Tér

### Paraméterrács

```mermaid
flowchart TD
    subgraph ENTRY["Entry cutoff"]
        E["0.90, 0.91, ..., 0.99\n10 szint\n= top 1–10% szignál sáv"]
    end
    subgraph TP["TP spec"]
        T["bucket_mean\nbucket_median\nbucket_p75\n0.75 × bucket_mean\n0.50 × bucket_mean\n5 szint"]
    end
    subgraph SL["SL spec"]
        S["none\n0.5× TP\n1.0× TP\n1.5× TP\n2.0× TP\n5 szint"]
    end
    TOTAL["~250 setup irányonként\n(10 × 5 × 5)"]
    ENTRY & TP & SL --> TOTAL
```

### Miért ez a keresési tér?

- **entry_cutoff 0.90–0.99:** Elég finom rács a top-sáv vizsgálatához, de még teljesen bejárható. A 0.90 alatti cutoff-ok túl sok gyenge szignált engednének be.
- **tp_specs bucket alapon:** A TP célpont közvetlenül a kalibrált bucket-statisztikákból ered — ez horgonyozza a TP-t a valós piaci MFE eloszláshoz. A szorzók (0.75×, 0.50×) konzervatív realizálást céloznak.
- **sl_specs TP-arányban:** A stop szélessége TP-arányban értelmezhető kockázati tengelyt ad — a "SL = 1.0× TP" például 1:1 risk-reward-ot jelent.

### Miért teljes grid és nem Optuna?

```mermaid
flowchart LR
    subgraph OPTUNA_PATH["Optuna (adaptív) — Elvetett"]
        O1["Kis térben: felesleges\n250 setup 1 perc alatt futtatható"]
        O2["Proxy objective-ot vinne be\nNem garantált egyezés a live viselkedéssel"]
        O3["Nem teljes lefedés\nEgyik setup el is maradhat"]
    end
    subgraph GRID_PATH["Teljes grid — Választott"]
        G1["Determinisztikus"]
        G2["Teljes lefedés — minden setup mérve"]
        G3["Könnyen auditálható"]
    end
```

A paramétertér tudatosan kicsi és előre zárt. A teljes grid futtatás reprodukálható és auditálható — bármikor meg lehet mondani, hogy az összes lehetséges setup közül melyik volt a legjobb és miért.

---

## Az Intrabar Végrehajtási Modell

### TP és SL aktiválási logika

```mermaid
flowchart TD
    ENTRY["Entry bar\n(signal tüzel)"]
    NEXT["Következő bar\nhigh és low vizsgálat"]
    TP_L{"Long TP:\nhigh >= entry × exp(tp_lr)?"}
    SL_L{"Long SL:\nlow <= entry × exp(-sl_lr)?"}
    BOTH{"Mindkettő\nugyanazon a báron?"}
    TP_EXIT["TP exit\n(realizált nyereség)"]
    SL_EXIT["SL exit\n(realizált veszteség)"]
    TO{"60 bar eltelt?"}
    CLOSE_EXIT["Timeout exit\n(bar close ár)"]

    ENTRY --> NEXT
    NEXT --> TP_L & SL_L
    TP_L & SL_L --> BOTH
    BOTH -- "mindkettő: SL wins" --> SL_EXIT
    BOTH -- "csak TP" --> TP_EXIT
    BOTH -- "csak SL" --> SL_EXIT
    TP_L & SL_L -- "egyik sem" --> TO
    TO -- "igen" --> CLOSE_EXIT
    TO -- "nem" --> NEXT
```

**Short irány:** Tükrözve — TP aktiválódik ha `low <= entry × exp(-tp_lr)`, SL ha `high >= entry × exp(sl_lr)`.

### Same-bar conflict rule: SL wins

Ha egy báron belül mind a TP, mind az SL szint érintett, a szimulációban az SL az érvényes exit.

**Indoklás:** Az intrabar sorrend (high vagy low volt-e előbb) ismeretlen OHLC adatból. Két opció:

| Döntés | Következmény |
|---|---|
| TP wins | Optimista bias — a valóságban az SL is aktiválódhatott előbb |
| **SL wins** | **Konzervatív bias — underestimálja a live PnL-t, nem felülbecsüli** |

A konzervatív döntés biztosítja, hogy a backtest nem fest szebb képet a valóságnál.

### Timeout és re-entry

- **Timeout:** 60 bar elteltével (ha sem TP, sem SL nem aktiválódott): close áron zárás
- **Re-entry:** Következő bar után azonnal lehetséges — nincs cooldown periódus

### Live vs. backtest különbség

| Elem | Backtest / Grid Search | Live Service |
|---|---|---|
| TP aktiválás | high/low touch (intrabar) | Nincs — timeout-only |
| SL aktiválás | low/high touch (intrabar) | Nincs — timeout-only |
| Same-bar rule | SL wins | Nem alkalmazható |
| Timeout | 60 bar (szimulált) | 60 perc (valós idő) |

A live intrabar bracket order monitoring külön epic feladata — jelenleg nincs implementálva.

---

## Üzleti és módszertani háttér

### Miért kritikus ez a lépés?

```mermaid
flowchart LR
    PROXY_OBJ["Proxy objective\n(pl. rank score, lift)"]
    EXEC_OBJ["Execution-aware objective\n(realizált fact_log_return)"]
    LIVE_BEHAVIOR["Live trading viselkedés"]

    PROXY_OBJ -.->|"Nem garantált egyezés"| LIVE_BEHAVIOR
    EXEC_OBJ -->|"Közvetlen megfelelés"| LIVE_BEHAVIOR
```

A strategy pipeline ezen a ponton fordítja le a kalibrált score-teret konkrét döntési szerződéssé — azt az egyetlen contract-ot, amelyet a live trading réteg változtatás nélkül használ. Ha az objective nem az, amit a live kereskedés mér, a laborban jónak tűnő setup élőben elvérzhet.

### Miért ezt a megközelítést?

| Megközelítés | Előny | Hátrány | Státusz |
|---|---|---|---|
| Teljes grid + realizált PnL objective | Determinisztikus, teljes lefedés, auditálható | Külön végrehajtási modellt kell fenntartani | ✅ Választott |
| TPE / Optuna proxy objective-val | Kevesebb kiértékelés nagy terekben | Kis térben felesleges; proxy elvi eltérést visz be | ❌ Elvetett |
| Kézi cutoff és TP/SL választás | Gyors emberi iteráció | Nem reprodukálható, könnyen torzított | ❌ Elvetett |
| Osztályozási/rank-metrikán optimalizálás | Egyszerűbb | Nem azonos a kereskedési eredménnyel | ❌ Elvetett |

### Paraméter alapértékek és indoklásuk

| Paraméter | Alapérték | Indoklás |
|---|---|---|
| `entry_cutoffs` | 0.90–0.99, 10 szint | Elég finom rács; teljes lefedés lehetséges |
| `tp_specs` | mean / median / p75 + 2 szorzó | Bucket-információra több realizációs agresszivitás |
| `sl_specs` | none, 0.5×, 1.0×, 1.5×, 2.0× TP | Jól értelmezhető kockázati tengely |
| `max_hold_bars` | 60 | Összhangban a fw60 target horizonnal |
| Same-bar szabály | SL wins | Konzervatív bias; ismeretlen intrabar sorrend |
| Kalibrálás / keresés szétválasztása | Külön időszak | Csökkenti az overfittinget a setup-választásban |
| Session architektúra | Dual session (irányonként) | Irányonként független cutoff optimalizálás |

### Ismert kockázatok és korlátok

| Kockázat | Tünet | Mitigáció |
|---|---|---|
| Ugyanazon rezsim túlfittelése | A setup új időszakon gyorsan romlik | Kalibrációs és keresési periódus szétválasztása; out-of-sample ellenőrzés |
| Kevés trade nagyon magas cutoffnál | Jó aggregate return, gyenge statisztikai megbízhatóság | Minimum trade-count figyelése a döntésnél |
| Same-bar szabály torzító hatása | Bizonyos setupok túl büntetettnek tűnnek | Konzervatív bias elfogadása; live-ban az intrabar sorrend nem ismert |
| Elavuló bucket-statisztikák | A TP-szintek már nem a jelenlegi rezsimet tükrözik | Rendszeres újrakalibrálás |
| Stop nélküli setup nagy drawdown-nal | Jó összhozam, rossz kockázati profil | Post-search kockázati review; elfogadási küszöbök |

### Validációs checklist

- [ ] A keresési periódus nem fedi át a kalibrációs periódust
- [ ] A grid search minden definiált cutoff × TP-spec × SL-spec kombinációt végigvizsgál
- [ ] A rangsor alapja a realizált `fact_log_return`, nem proxy metrika
- [ ] A same-bar konfliktus kezelése explicit és konzervatív (SL wins)
- [ ] A short irány belépési logikája invertált percentilis-szemantikát használ: `(1 - score_pct_short) >= cutoff`
- [ ] A kiválasztott setup trade-száma elégséges az aggregate metrika értelmezéséhez
- [ ] A grid search irányonként külön fut: long és short session önálló cutoff-fal
- [ ] A két session neve és cutoff értéke rögzítve van az artifact manifest-ben
- [ ] Nincs kombinált session shared cutoff-fal (kivezetett architektúra)
