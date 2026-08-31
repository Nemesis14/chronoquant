---
epic: epic_051
id: t3
title: Normalizálás — TradingView raw réteg → forrásfüggetlen calls tábla
assignee: —          # főszálon fut, agent nélkül (a t1/t2 folytatása)
status: done
blocks: [t4]
blocked_by: []
---

## Goal

A `t2` nyers TradingView rétegét forrásfüggetlen `call` eseményekké alakítani,
a [plan.md](../../plan.md) 3. szakaszának absztrakciója szerint — hogy a
scoring réteg (T5) semmit ne tudjon a TradingView-ról, és az `epic_052`
(Reddit) meg az `epic_050` (leaderboard) ugyanabba a táblába írhasson.

## Scope

```
src/data_handling/store/analyst_signal_store.py     ← forrásfüggetlen store
src/data_handling/collectors/tradingview/normalize.py ← TV-specifikus leképezés
database/analyst_signal/analyst_signal.duckdb        ← calls tábla
```

**Külön adatbázis, nem a `btcusdt.duckdb`-ben** (user döntés): a call
forrás-scoped, nem asset-scoped, és a `btcusdt` store később quantitative
munkára is megy — ne keveredjen bele analitikai esemény.

## Döntések (user, 2026-08-17)

1. **Irány nélküli ideák nem kerülnek be.** Egy call definíció szerint irányos.
   A raw réteg megőrzi őket, a T5 onnan számolhat szelektivitás-jelet, ha kell.
2. **Perpetualok bekerülnek**, `asset='BTCUSDT'`-ként. Az ár a spothoz tapad.
3. **Külön DB**, lásd fent.

## A négy lépés

**szűrés** — whitelist a `symbol.short_name`-re, `is_education`/`is_script`
kizárás, `direction ∈ {1,2}` követelmény
**leképezés** — TV mező → `plan.md` call mező
**dekódolás** — `direction: 2` → `-1`, `interval: "1D"` → `1440` perc
**azonosítás** — `call_id = sha1("tradingview|{native_id}")`

## Acceptance Criteria

- [x] `calls` tábla létrejön verziózott migrációval (`SIGNAL_DB_MIGRATIONS`)
- [x] Szimbólum-whitelist, nem substring-illesztés
- [x] `ts_utc` **mindig** a `created_at`, soha nem az `updated_at`
- [x] `horizon_hint_min` percre normalizálva, range-baroknál NULL
- [x] `call_id` idempotens, `INSERT OR REPLACE` a kulcsra
- [x] `raw_ref` + `first_seen_utc` minden soron (visszakövethetőség)
- [x] `ruff check` + `pyright` tiszta

## Notes

### A whitelist a kritikus rész

A `t2` review `is_btc()` szűrője substring-illesztést használt („BTC" a
`short_name`-ben), és **túl sokat fogott be**. A kizártak, irányos + nem
education szűrés után:

```
BTC.D          162   Bitcoin dominancia INDEX — nem ár
ETHBTC         112   ETH BTC-ben — a "long" itt nem BTC-long
BTC1!           91   CME folytonos futures — bázis + roll artefakt
XRPBTC, XLMBTC, LTCBTC, ADABTC, …   ~400   altcoin/BTC párok
BTCUSDSHORTS    11   pozicionáltsági indikátor
```

Egy `long ETHBTC` azt állítja, hogy **az ETH veri a BTC-t** — BTC-longként
mérve ez akár fordított előjel is lehet.

Whitelist: `BTCUSD`, `BTCUSDT`, `BTCUSDT.P`, `BTCUSD.P`, `BTCUSDC.P`, `BTCPERP`.
A `BTCPERP` a 2. döntés következetes alkalmazása. A `BTC1!` szándékosan kint
marad: más instrumentum-osztály.

### Az `interval` vegyes kódolású

40+ distinct érték: bare percek (`240`, `60`, `32`, `233`), napok (`1D` és `D`),
hetek (`1W`, `W`), hónapok (`1M`), órák (`12H`), és **range bar** (`8192R`,
`16384R`) — ami nem időhorizont. Ezért van két oszlop: `horizon_hint_min`
(percre normalizálva, range-barnál NULL) és `horizon_hint_raw` (eredeti).

Eredmény: **38 NULL / 17 061** — a range-barok.

### A `call_id` nem szerző+idő alapú

A raw rétegben **24 olyan pár van, ahol ugyanaz a szerző ugyanabban a percben
posztolt**. Egy (author, minute) kulcs ezeket némán összeolvasztaná. Ezért a
TV saját `id`-jára épül.

---

### Eredmény — 2026-08-17

```
nyers idea                144,966
  symbol nem whitelisten -116,675   (XAUUSD 16k, EURUSD 7k, … más piacok)
  is_education              -921
  no_direction           -10,323
─────────────────────────────────
call                       17,061      long 10,243 / short 6,818
```

- **636 szerző**, időszak **2015-03-04 .. 2026-08-17**
- `horizon_hint_min` NULL: 38/17 061 · `first_seen_utc` NULL: **0/17 061**
- `distinct call_id` = `distinct native_id` = 17 061 → nincs ütközés

**Mérhető populáció:**

| küszöb | szerző | call |
|---|---:|---:|
| ≥ 10 | 272 | 15 809 |
| ≥ 30 | **139** | 13 577 |
| ≥ 100 | 40 | 7 949 |

A szigorú whitelist miatt kissé kevesebb, mint a `t2` review becslése
(281 / 146) — ez a helyes irány, nem veszteség.
