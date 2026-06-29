# 6000 — Strategy Áttekintés

## Overview

A strategy domain a két végső modell nyers score-ját alakítja át olyan döntési szerződéssé, amelyet a trading runtime már végre tud hajtani. Ez nem új modell-tanítás, hanem score-értelmezés, kalibrálás és szabálykeresés.

```mermaid
flowchart TD
    PRED["Long és short modell offline predikciói\npred_long_raw, pred_short_raw"]
    SCORE["Strategy scored tábla\nnyers score-ok együtt"]
    CAL["Score kalibrálás\nrank percentile + isotonic\n→ 6100_strategy_calibration.md"]
    SEM["Short score szemantika\ninvertált logika konzisztens kezelése\n→ 6200_short_score_semantics.md"]
    OPT["Entry/exit keresés\nexecution-aware grid search\n→ 6300_strategy_grid_search.md"]
    ART["Strategy artifact\nentry_cutoff + tp_spec + sl_spec\nirányonként külön"]
    LIVE["Live trading runtime\ncsak artifact-ot fogyaszt\nnem kalibral újra"]

    PRED --> SCORE --> CAL --> SEM --> OPT --> ART --> LIVE
```

---

## A három lépés szétválasztása

### Miért három önálló lépés?

```mermaid
flowchart LR
    subgraph C["Kalibrálás\n(értelmezés)"]
        C1["Raw score → percentile\nMit jelent ez a szám?"]
    end
    subgraph G["Grid search\n(döntéshozás)"]
        G1["Percentile → entry feltétel\nMikor lépjünk be?"]
        G2["MFE bucket → TP/SL spec\nHol zárjunk?"]
    end
    subgraph L["Live végrehajtás\n(csak fogyasztás)"]
        L1["Artifact alapján dönt\nNem tanul újra, nem kalibral"]
    end
    C --> G --> L
```

A három szint szétválasztása kritikus:

- A **kalibrálás** értelmezi a score-t — offline, a kalibrációs periódus adatain fut
- A **grid search** keresi a legjobb decision rule-t — offline, realizált P&L alapján
- A **live runtime** csak a kész artifact-ot fogyasztja — nem hoz új döntést

Ha a live runtime kalibrálna vagy optimalizálna, az overfitting és operációs instabilitás kockázatát vinné be a rendszerbe.

### Long és short szimmetria — de szemantikai inverzióval

A long és short irány teljesen szimmetrikus struktúrájú: mindkettőnek van raw score-ja, percentile kalibrációja, bucket statisztikája, entry cutoff-ja és TP/SL spec-je. A tartalom azonban szemantikailag ellentétes:

```mermaid
flowchart LR
    subgraph LONG["Long irány"]
        LS["Magas pred_long_raw\n→ magas score_pct_long\n→ erős long szignál"]
        LE["score_pct_long >= cutoff\n→ ENTER_LONG"]
    end
    subgraph SHORT["Short irány — Invertált"]
        SS["Alacsony pred_short_raw\n→ alacsony score_pct_short\n→ erős short szignál"]
        SE["(1 - score_pct_short) >= cutoff\n→ ENTER_SHORT"]
    end
    LONG -.->|"Szimmetrikus struktúra\nellentétes szemantika"| SHORT
```

Az invertált logika részletes magyarázata: → `6200_short_score_semantics.md`

---

## Domain-szintű elvek

### A rank-first elsődlegessége

Az elsődleges döntési nyelv a percentile és a bucket-expectancy, nem a raw score. A raw score skálája modellfüggő és rezsimfüggő — ugyanaz a numerikus érték két különböző időszakban más helyi erősséget jelenthet. A percentilis minden session-re azonos 0–1 skálán hozza ki az erősséget.

### Offline kalibrálás — live változatlanság

A kalibrációs artifact (rank lookup, isotonic görbék, bucket statisztikák) offline épül, és a live runtime változatlanul használja. Ez garantálja, hogy a live kereskedés determinisztikus és auditálható.

### Dual-session architektúra

```mermaid
flowchart LR
    subgraph LONG_SESSION["Long session"]
        LS_ID["strat_solusdt_fw60_long_2101_2605"]
        LS_CUT["cutoff = 0.98 (top 2%)"]
        LS_STAT["78 trade, 79.5% win rate\n+50.1% compounded"]
    end
    subgraph SHORT_SESSION["Short session"]
        SS_ID["strat_solusdt_fw60_short_2101_2605"]
        SS_CUT["cutoff = 0.94 (top 6%)"]
        SS_STAT["260 trade, 62.3% win rate\n+22.7% compounded"]
    end
```

A long és short session egymástól teljesen független: külön kalibrálás, külön grid search, külön artifact. Ez lehetővé teszi, hogy az egyik irány újra-optimalizálásakor a másikat ne kelljen érinteni.

---

## Alfejezetek

| Fájl | Szerep |
|------|--------|
| `6100_strategy_calibration.md` | Score → percentile, bucket és isotonic interpretáció |
| `6200_short_score_semantics.md` | Invertált short logika teljes módszertani indoklása |
| `6300_strategy_grid_search.md` | Execution-aware grid search — determinisztikus TP/SL keresés realizált P&L objektívvel |

---

## Ismert kritikus gyenge pontok

| Gyenge pont | Tünet | Mitigáció |
|---|---|---|
| Same-window backtesting | A kalibrációs és keresési periódus részben átfed | Kalibrációs és keresési ablak explicit szétválasztása |
| Rezsimváltás elavítja a kalibrációt | A rank lookup percentilis-értékei más minőséget képviselnek | Rendszeres re-kalibrálás, különösen rezsimváltás után |
| Long és short score aszimmetria | Azonos cutoff más minőséget jelent a két irányban | Dual-session architektúra — irányonként független cutoff |
| Live TP/SL hiány | A backtest intrabar TP/SL-t szimulál, a live csak timeout-ot implementál | Explicit dokumentálás; live implementálás külön epic feladata |
