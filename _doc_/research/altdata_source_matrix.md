# Alt-data forrásmátrix — derivatívák (SOLUSDT)

> **Státusz:** felmérés lezárva — 2026-08-21 (epic_054 / t301)
> Minden érték **élő lekérdezéssel ellenőrizve**, nem dokumentációból átvéve.

---

## Összefoglaló

A derivatíva-ághoz **nincs szükség fizetős szolgáltatóra**. A Binance Vision
archívum ~20 MB letöltéssel 4,7 év, 5 perces pozicionáltsági adatot ad —
mélyebben, mint amit a CoinGlass havi $699-ért kínál.

Egyetlen adat nem szerezhető be visszamenőleg: a **likvidációk**.

---

## 1. Felbontás — a fő korlát

Pozíció-aggregátum **1 perces felbontásban sehol nem érhető el**:

| Forrás | 1m kérés eredménye | Legfinomabb |
|---|---|---|
| Binance `/futures/data/openInterestHist` | `[]` (HTTP 200, üres) | 5m |
| Bybit `/v5/market/open-interest` `intervalTime=1min` | `list: []` | 5min |
| OKX `rubik/stat/contracts/*` | — | 5m |

⚠️ Az üres tömb **nem hiba** — egy naiv collector ezt "nincs új adat"-ként
naplózná és hónapokig gyűjtene semmit.

**1 perc két úton érhető el, de csak előre:**
- `/fapi/v1/openInterest` — valós idejű OI snapshot, percenkénti pollinggal
- `aggTrades` — tick szintű, ebből 1m aggregátum származtatható

---

## 2. Binance Vision archívum — ingyenes, bulk, backfillelhető

| Adat | Felbontás | Mettől | Méret | Tartalom |
|---|---|---|---|---|
| `metrics/SOLUSDT` | **5m** | **2021-12-01** | ~20 MB | OI (coin + USD), 3 long/short arány, taker vol ratio |
| `premiumIndexKlines` | 1m/5m | 2020-09 | kicsi | prémium index OHLC = (mark − index)/index |
| `markPriceKlines` / `indexPriceKlines` | 1m/5m | 2020-09 | kicsi | mark ár, spot index |
| `bookDepth` | ~31 mp | **2023-01-01** | ~600 MB | mélység ±1/2/3/4/5%-on (coin + notional) |
| `aggTrades` (futures) | tick | 2020-09 | ~10–15 GB | trade-méret eloszlás |
| `fundingRate` (monthly) | 8h | 2020-09-13 | apró | tényleges funding |
| spot `klines` 1m + `aggTrades` | 1m / tick | 2020-08 | — | spot volumen, bázis |

**Ellenőrzött részletek:**
- `metrics` napi fájl = 288 sor + fejléc (5 perces rács, éjféltől éjfélig)
- Legkorábbi: `SOLUSDT-metrics-2021-12-01.zip`; 2021-11-30 → HTTP 404
- Frissesség: T-1 (2026-08-20 elérhető, 2026-08-21 még nem)
- `bookDepth`: 2739 snapshot/nap (~31 mp), 10 szint (±1..±5%)

**`metrics` oszlopok:**
```
create_time, symbol, sum_open_interest, sum_open_interest_value,
count_toptrader_long_short_ratio, sum_toptrader_long_short_ratio,
count_long_short_ratio, sum_taker_long_short_vol_ratio
```

⚠️ Nincs `sum_long_short_ratio` oszlop — az az összes számla pozíció-méretének
aránya lenne, ami definíció szerint **konstans 1,0** (minden szerződésnek van
egy long és egy short oldala). Nulla információ, ezért nem publikálják.

---

## 3. REST — nem backfillelhető

| Adat | Endpoint | Visszamenőleges ablak |
|---|---|---|
| OI, L/S arányok, taker ratio | `/futures/data/*` | **~30 nap** (25 nap még ad, 40 nap → `parameter 'startTime' is invalid`, kód -1130) |
| OI snapshot | `/fapi/v1/openInterest` | nincs (pillanatnyi) |
| **Likvidációk** | `!forceOrder` websocket | **nincs, és archívum sem létezik** |

A Vision `futures/um` alatt **sem daily, sem monthly** szinten nincs
`liquidationSnapshot` — a teljes típuslista ellenőrizve. A likvidáció az
egyetlen valóban "ma vagy soha" elem.

⚠️ A Binance 2021 óta throttle-olja a likvidációs streamet (~1 esemény/mp),
tehát a gyűjthető adat szisztematikusan alulmér — és nagy kilengéskor a
legrosszabbul, épp amikor számítana.

---

## 4. Egyéb tőzsdék

| Forrás | Felbontás | Historikus mélység | Megjegyzés |
|---|---|---|---|
| Bybit `/v5/market/open-interest` | 5min | **legalább 2023-01-01** (ellenőrizve) | keresztvalidációra alkalmas |
| OKX `rubik/stat/*` | 5m | gyakorlatilag nincs (`Illegal time range` 2025-01-re) | historikusan használhatatlan |

⚠️ **Konvenció-csapda.** A Bybit ugyanarra a SOLUSDT-re két számot ad:
`openInterest: 6042789` és `singleOpenInterest: 3021394,5` — pontosan a duplája.
Az egyik mindkét oldalt számolja, a másik egyet. Több tőzsde összeadása előtt
tisztázni kell, melyik konvenciót használja a Binance, különben kétszeres hiba
kerül a modellbe.

---

## 5. Fizetős szolgáltatók

### CoinGlass

| Tier | Ár/hó | Rate limit | 1 perces history | Napi history |
|---|---|---|---|---|
| Hobbyist | $29 | 30/min | 6 nap | all-time |
| Startup | $79 | 80/min | 12 nap | all-time |
| Standard | $299 | 300/min | 3 nap | all-time |
| Professional | $699 | 1200/min | — (720 nap órás+) | all-time |
| Enterprise | egyedi | — | CSV/bulk export | — |

**A mélység a felbontással fordítottan arányos.** Az "all-time" mindenhol
**napi** felbontásra vonatkozik. A 4,7 éves, 5 perces ablakunkat egyik tier sem
adja meg — a Vision ingyen igen.

Amit hozzáadna: **likvidációs adat** (order flow, heatmap), multi-exchange
aggregát OI, exchange balance/netflow, opciós és ETF flow.

### CryptoQuant

| Tier | Ár/hó (éves) | API felbontás |
|---|---|---|
| Advanced | $29 ($39 havi) | nincs API |
| Professional | $99 ($109 havi) | **max 24 óra** |
| Premium | $699 ($799 havi) | block-level |

**Kizárva, két okból:**
1. A **Solana nincs a támogatott asset-listán** (BTC, ETH, XRP, TRX, stablecoin,
   ERC20; altcoinokra csak market data).
2. $99-ért napi felbontás — egy napi érték a `fw60` targethez konstans, nem
   hordoz órán belüli információt.

---

## 6. Funding rezsim — mért kontextus

A funding **rezsim-jelző, nem szint-jelző** (SOLUSDT, `/fapi/v1/fundingRate`):

| Időszak | Pozitív funding | Évesítve | Max / 8h |
|---|---|---|---|
| 2021 bull | 95,0% | **+30,3%** | 0,3325% |
| 2022 bear | 60,4% | **−32,3%** | 0,0100% |
| 2023 | 69,2% | −0,9% | 0,0513% |
| 2024 bull | 90,8% | **+13,8%** | 0,1193% |
| 2025 | 59,8% | +0,1% | 0,0259% |
| 2026 | 48,7% | −1,9% | 0,0100% |

A medve években a maximum pontosan `0,0100%` = a Binance alap-kamatkomponense,
vagyis a funding **egyszer sem emelkedett a semleges szint fölé**.

**Következmény a modellezésre:** nyers `funding_rate` feature a 2021→2026
tanítási ablakon korszak-proxyt tanulna, nem pozicionáltságot. A `0,0100%`
alapértéktől mért **eltérés** a használható jel, trailing normalizálással.

---

## 7. Döntés

| Csoport | Döntés |
|---|---|
| Binance Vision `metrics` + `premiumIndex` + spot | **IGEN** — Tier 1+2, 22 változó, azonnal |
| `bookDepth` | később — 2023-01-tól, második lefedettségi határt vezetne be |
| `aggTrades` | később — 10–15 GB feldolgozási költség |
| Likvidációs collector | **IGEN, párhuzamosan** — nem a kritikus úton, de visszafordíthatatlan |
| Bybit keresztvalidáció | pozitív gate után |
| CoinGlass / CryptoQuant / OKX | **NEM** |

**Adatablak a modellezéshez:** 2022-01-01 → (a `metrics` 2021-12-01-es kezdete
miatt), valid a championnel azonos: 2026-02-01 → 2026-07-31.

---

## Kód a git historyban

A kísérleti kód a repóból törlődik (epic_068, t894/t895). Törlés előtti állapot: commit `73f2156`, útvonalak: `src/data_handling/collectors/derivatives/{vision,liquidations,load}.py`.
