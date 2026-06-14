# Elliott Wave 1-2-1-2 Bullish Setup — Kutatási összefoglaló

## Tartalom

| File | Description |
|---|---|
| `algorithmic_spec.md` | Full algorithmic specification: theory, pivot definitions, rules, scoring, parser plan |
| `wave4_completion_study.md` | Wave 4 completion event study and results |
| `overview.md` | This overview |
---

## Mi a cél

A **nested 1-2-1-2 bullish setup** azonosítása SOLUSDT 1m OHLCV adatokon.  
A setup lényege: két egymásba ágyazott Elliott Wave 1-2 struktúra, ahol a P4 mélypont után indul a Wave 3 — a trend gyorsuló szakasza.

```
P0 (low) → P1 (high) → P2 (low) → P3 (high) → P4 (low)
              Wave 1      Wave 2        sub-1       sub-2
```

---

## Kiválasztott módszer

### Pivot motor: ZigZag threshold

A `algorithmic_spec.md` három lehetőséget tárgyal (local extrema, ZigZag, hibrid).  
**Választás: ZigZag threshold** — a spec 5B/5C fejezete alapján.

Működés:
- Swing low megerősítve, ha close felfelé `threshold`%-ot mozog a mélyponttól
- Swing high megerősítve, ha close lefelé `threshold`%-ot esik a csúcstól
- A pivot időpontja = az extremum bar; a megerősítés = confirmation bar
- **Nincs lookahead bias** — csak megerősített pivot szerepel a logikában

Miért nem local extrema (Williams fraktál)?  
1m adaton túl sok a zaj, minden kis kanóc pivotnak számít.

### Timeframe: 1m → 5m aggregáció

Az 1m OHLCV adatból (`solusdt_1m`) 5 perces gyertyákat képzünk.  
A durációs limitek 5m-es barokban értendők (pl. D1=5–50 bar = 25 perc – 4 óra).

Más timeframe-ek eredménye a teljes 2020–2026 historyn:

| TF | Threshold | Setupok | Score ≥ 0.4 |
|---|---|---|---|
| **5m** | **0.010** | **511** | **464** |
| 5m | 0.008 | 700 | 643 |
| 10m | 0.010 | 347 | 328 |
| 15m | 0.012 | 245 | 228 |
| 30m | 0.015 | 124 | 112 |

**Referencia variáns: 5m / threshold=1.0%** — legjobb egyensúly jel–zaj arányban.

---

## Feltételrendszer (spec 6.3 alapján)

### Hard strukturális szabályok (Elliott hard rule)

```
P1 > P0          # big Wave 1 felfelé megy
P2 > P0          # big Wave 2 nem törli a Wave 1 kezdőpontját
P3 > P2          # sub Wave 1 felfelé megy
P4 > P2          # sub Wave 2 nem törli a sub Wave 1 kezdőpontját
P4 < P3          # sub Wave 2 valóban visszahúzás
```

### Fibonacci retrace szűrő

```
R_big = (P1 - P2) / (P1 - P0)   →  0.382 ≤ R_big ≤ 0.854
R_sub = (P3 - P4) / (P3 - P2)   →  0.382 ≤ R_sub ≤ 0.854
```

Ideális (magas score): R értékek közel 0.500 vagy 0.618-hoz.

Valódi eloszlás a detektált setupokon (5m/0.010):
- Medián R_big = **0.585**, medián R_sub = **0.688** — mindkettő a 0.5–0.618 Fibonacci zónában

### Amplitúdó szűrő

```
W1_big = P1 - P0  ≥  1.0 × ATR14   (nagyobb fokozatú hullám)
W1_sub = P3 - P2  ≥  0.5 × ATR14   (beágyazott hullám)
W1_sub < W1_big                     (nested struktúra)
```

### Durációs szűrő (5m barokban)

| Leg | Min | Max | Időben |
|---|---|---|---|
| D1 = P0 → P1 | 5 | 50 | 25 perc – 4 óra |
| D2 = P1 → P2 | 3 | 50 | 15 perc – 4 óra |
| D3 = P2 → P3 | 3 | 30 | 15 perc – 2.5 óra |
| D4 = P3 → P4 | 2 | 30 | 10 perc – 2.5 óra |
| Teljes (P0→P4) | 10 | 120 | 50 perc – 10 óra |

---

## Scoring (0–1 skála)

```
score = 0.50 × fib_score + 0.25 × duration_score + 0.25 × amplitude_score
```

| Komponens | Számítás |
|---|---|
| `fib_score` | Mindkét retrace közelsége 0.500 / 0.618-hoz, átlagolva |
| `duration_score` | Leg-hosszok nem extrémek (1–2 bar túl rövid, 50+ túl hosszú) |
| `amplitude_score` | W1_sub / W1_big arány közel 0.5-höz (ideális nested struktúra) |

Score eloszlás (5m/0.010, n=511):  
- Mean = 0.60, Std = 0.14, Min = 0.25, Max = 0.98
- Score ≥ 0.7: 131 setup (26%)

---

## P4 megerősítés — lookahead-mentes trade timing

**Kritikus pont:** a P4 nem a tényleges mélypont barján lesz ismert, hanem csak a confirmation barján.

```
P4 low bar     = a tényleges swing low (nem trade-elhető)
P4 conf bar    = az első bar ahol close ≥ P4_price × (1 + threshold)
               = innentől valid a P4, innentől létezik a setup
```

Confirmation lag eloszlás (5m / 0.010, összes low pivot):

| Percentilis | Lag (bar) | Lag (perc) |
|---|---|---|
| 25% | 1 | 5 perc |
| 50% (medián) | 2 | 10 perc |
| 75% | 5 | 25 perc |
| Példa (2026-06-08) | 14 | 70 perc |

A `detect_1212()` output `conf_time` oszlopa = a helyes signal idő.  
`p4_time` = csak tájékoztató (hol volt a mélypont).

### Entry trigger (spec 6.4 alapján)

```
Agresszív:    close > P3 + buffer     (conf_bar után első ilyen bar)
Konzervatív:  close > P1 + buffer     (teljes big Wave 1 törésekor)
buffer        = max(0.1 × ATR14, 1–2 tick)
```

---

## Codebase

| File | Role |
|---|---|
| `src/elliott_waves/elliott_1212.py` | Legacy/simple 1-2-1-2 detector entry point |
| `src/elliott_waves/elliott/` | Modular Elliott engine: pivots, validators, scanners, parser, viz, backtest |
| Legacy script entry points | Removed; use maintained `src/` modules or recreate scripts on the current Parquet/DuckDB layer |

---

## Következő lépések

1. **Forward outcome labeling** — elért-e a P4 conf után `1.618 × W1_sub` célszintet mielőtt stopot ért?
2. **Precision / recall** — hány setup után jött valóban Wave 3 jellegű emelkedés?
3. **Score threshold optimalizálás** — melyik score-határ felett a legjobb a precision?
4. **Feature engineering** — a setup paraméterek (R_big, R_sub, score, lag, ATR) használata ML modell bemeneteként
