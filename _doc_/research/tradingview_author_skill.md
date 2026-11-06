# Kutatás: TradingView szerző-skill — van-e követhető elemző?

**Cél:** Eldönteni, hogy publikus platformon vannak-e olyan szerzők, akiknek az
irányos BTC-előrejelzései **perzisztensen** prediktívek, és akiket ezért érdemes
lenne feature-ként becsatornázni a modellbe.

**Időszak:** 2015-03 – 2026-08 (a callok), 2017-08 – 2026-08 (az árfolyam)
**Epic:** `epic_051_tradingview_calls` · **Lefutva:** 2026-08-17 – 2026-08-20

> **Döntés: NEM MEGY.** Nincs azonosítható szerző, akit érdemes lenne követni.
> Az `epic_052` (Reddit) a gate szerint törlődik.

---

## Háttér és hipotézis

> Publikus platformokon vannak olyan szerzők, akiknek az irányos előrejelzései
> perzisztensen prediktívek egy adott assetre — és ez out-of-sample is
> kimutatható, nem csak utólagos szerencse.

A TradingView Ideas azért volt az első vizsgálat, mert ott az
`(author, asset, direction, timestamp)` négyes **natívan** megvan: a szerző maga
jelöli meg a posztot Long/Short-ként. Nincs szövegértelmezés, tehát nincs
extraktor-hiba, ami elmoshatná a mérést. Ha itt nincs jel, akkor a Redditen —
ahol a címkét mi gyártanánk, nagyságrenddel több munkával — végképp nincs.

**BTC-first döntés (user, 2026-08-17):** a validáció kizárólag BTC-n fut. A BTC
a legnépszerűbb, legtöbb szerzővel lefedett asset; ha itt nincs jel, a SOL-on
végképp nem lesz.

---

## Módszer

### 1. Adatgyűjtés

A HTML-alapú scraping **zsákutca volt**: a szimbólum-feed csak ~10 napot ad
vissza, a szerzői oldal pedig JS-renderelt, adat nélküli váz.

A megoldás egy fel nem dokumentált, de anonim módon elérhető REST endpoint,
amit a TradingView saját JS bundle-jében találtunk meg:

```
GET /api/v1/ideas/?symbol=&by={username}&page=&per_page=100
→ { count, page_count, results[] }
```

Egyetlen hívás adja a `created_at`-et **és** az `updated_at`-et külön, a
`symbol.direction`-t, a `symbol.interval`-t és a `user.id`-t.

**Két fázis:** felderítés `symbol=BTCUSD|BTCUSDT` lekérdezéssel (~2000 idea,
741 aktív szerző), majd szerzőnkénti teljes history `by={username}`-nel.
**1913 kérés, 191 perc, 397 MB, nulla rate-limit.**

### 2. Egy leak, amit el kellett kerülni

A listaoldal az **`updated_at`**-et mutatja, nem a publikálás idejét. Egy
konkrét ideán **90,2 óra** volt az eltérés:

```
kártya:      2026-08-12T05:23:34   ← updated_at
detail JSON: 2026-08-08T11:13:48   ← created_at, az igazi publikálás
```

`h=1h` horizonton ez nem pontatlanság, hanem közvetlen lookahead leak. A
normalizálás ezért **mindenhol a `created_at`-et** használja.

### 3. Normalizálás — `calls` tábla

Forrásfüggetlen `call` esemény, hogy a scoring réteg ne tudjon a
TradingView-ról. **17 061 call** 144 966 nyers ideából.

A szűrés kritikus része a **szimbólum-whitelist**, nem substring-illesztés.
Egy „BTC" tartalmú név nem feltétlenül BTC-árfolyam:

| kizárva | n | mi ez |
|---|---:|---|
| `BTC.D` | 162 | Bitcoin dominancia **index** |
| `ETHBTC` | 112 | ETH BTC-ben — a long **nem** BTC-long |
| `BTC1!` | 91 | CME folytonos futures |
| altcoin/BTC párok | ~400 | `XRPBTC`, `LTCBTC`, … |
| `BTCUSDSHORTS` | 11 | pozicionáltsági indikátor |

Whitelist: `BTCUSD`, `BTCUSDT`, `BTCUSDT.P`, `BTCUSD.P`, `BTCUSDC.P`, `BTCPERP`.

### 4. Forward return + drift-korrekció

BTCUSDT **spot**, 1 perces, 4 729 212 gyertya (2017-08-17-től, 0,18% hiány).
Horizontok: 1h, 4h, 1d, 3d, 7d.

**A drift-korrekció nem opcionális.** A callok 61,5%-a long, minden évben 50%
felett. Emelkedő piacon a nyers hozam az **irány-torzítást** rangsorolná:

| h | drift | long nyers | short nyers | különbség |
|---|---:|---:|---:|---:|
| 1h | 0,005% | +0,006% | −0,005% | 0,01 pp |
| 1d | 0,056% | +0,039% | −0,070% | 0,11 pp |
| 7d | 0,404% | +0,321% | −0,530% | **0,85 pp** |

Két független becslés készült — keresztmetszeti (havi bucket) és szigorúan
leak-mentes (trailing 365 nap) —, **korrelációjuk 0,88–0,99**, tehát az
eredmény nem a módszerválasztáson múlik.

Emellett volatilitás-normalizálás (`z`), mert a BTC vol korszakonként
nagyságrendekkel tér el.

### 5. Scoring és validáció

Empirical Bayes shrinkage (`k = sigma2/tau2`, **mindkettő becsülve**), rolling
out-of-sample rangsor nem átfedő havi ablakokkal, majd permutációs placebo
battery + Benjamini-Hochberg FDR.

A permutáció **megőrzi a szerzőnkénti call-számot**, tehát egy pusztán
mintaméret-vezérelt rangsor nem álcázhatja magát képességnek.

---

## Eredmények

### A mezőny érmedobás

| | 1h | 4h | 1d | 3d | 7d |
|---|---|---|---|---|---|
| long találat | 50% | 50% | 51% | 50% | 50% |
| short találat | 49% | 48% | 47% | 47% | 47% |

Az **átlagos call alulteljesíti a buy-and-holdot**, és ez a horizonttal romlik.

### A `tau2` gyakorlatilag nulla

A szerzők **közötti** valódi skill-szórás nem különböztethető meg a nullától:

| h | `tau2 > 0` a fitekben | medián `tau2` | `sigma2` | implikált `k` |
|---|---:|---:|---:|---:|
| 1h | **1/55** | 0,00000 | 1,12 | ∞ |
| 1d | 28/55 | 0,00023 | 1,08 | **~4 700** |
| 3d | 50/55 | 0,01014 | 0,98 | ~97 |

`k ≈ 4 700` azt jelenti: egy szerzőnek ~4 700 callra lenne szüksége, hogy a
saját átlaga feleakkora súlyt kapjon, mint a populációé. A legtermékenyebbnek
716 van, a mediánnak 7.

### Placebo battery — 138 szerző, ≥30 call

Minden teszt a valódi adatot **permutált szerző-címkékhez** hasonlítja
(5000 permutáció, call-számok megőrizve):

| h | teszt | valódi | véletlen | p |
|---|---|---:|---:|---:|
| 1h | buy-and-holdot veri (db) | 67 | 70,3 | 0,816 |
| 1h | egyénileg szignifikáns (db) | 0 | 3,5 | 1,000 |
| 1h | legjobb `t` | 1,92 | 2,65 | 0,989 |
| **1d** | buy-and-holdot veri (db) | **60** | **62,6** | 0,760 |
| **1d** | egyénileg szignifikáns (db) | **2** | **2,4** | 0,708 |
| **1d** | legjobb `t` | **2,50** | **2,52** | **0,476** |
| 7d | buy-and-holdot veri (db) | 52 | 53,4 | 0,662 |
| 7d | legjobb `t` | 2,14 | 2,40 | 0,706 |

**A nyertesek száma minden horizonton a véletlen szintjén vagy alatta van.**

### Egyéni szerzők — FDR

| h | nyers `p<0,05` | véletlenül várható | **FDR 10%** | **FDR 5%** |
|---|---:|---:|---:|---:|
| 1h | 5 | 6,9 | **0** | **0** |
| 4h | 10 | 6,9 | **0** | **0** |
| 1d | 6 | 6,9 | **0** | **0** |
| 3d | 4 | 6,9 | **0** | **0** |
| 7d | 6 | 6,9 | **0** | **0** |

**Egyetlen szerző sem éli túl a korrekciót, egyetlen horizonton sem.**

### A legjobb jelölt

`P_S_trade`, 1d: **+1,359%/call**, 40 call, `t = 2,50`, nyers `p = 0,0062`.
Önmagában publikálható lenne. Korrekció után:

- FDR-korrigált `p = 0,849`
- **Family-wise permutációs `p = 0,476`** — a 138 *hamis* szerző legjobbika az
  esetek 48%-ában ilyen jó vagy jobb

A short-találatai „75%"-a **3 találat 4-ből**.

### A kiugró számok mintázata

A top-10 szerző cellái végignézve mindig ugyanazt mutatják: **ami magas, az
kicsi** (`3/4`, `8/10`, `9/11`), **ami nagy mintájú, az 50% körül tapos**. És
senki nem tartja magát horizontok vagy irányok között — a tipikus minta, hogy
az egyik irány 60% fölött, a másik 40% alatt, ami nem tudás, hanem egy piaci
fázisban következetesen egy irányba szóló szerző.

Egy 36 jelzéses szerzőnek **~64% fölé** kellene mennie, hogy egyáltalán
szignifikáns legyen. Ilyen nincs.

---

## Döntés

**`epic_051` → NEM MEGY.**

**`epic_052` (Reddit) → törölve.** A gate logikája: a TradingView-n a címke
tökéletes, mi nem gyártjuk. Ha itt nincs jel, a Redditen — extraktor-zajjal,
nagyságrenddel több munkával — végképp nem lesz.

**`epic_050` (leaderboard) → fut tovább, ettől függetlenül.** Ott nem publikált
véleményeket mérünk, hanem **valódi pozíciókat valódi tétekkel**. Az „aki
nyilvánosan okoskodik" és az „aki saját pénzzel következetesen nyer" két
különböző populáció; ez az eredmény az elsőről szól.

---

## Limitációk — mit NEM mond ki ez a kutatás

**Nem azt, hogy senkinek nincs képessége.** Azt, hogy **senki nem azonosítható**.

Erő-elemzés: egy 76 callos szerzőnél a call-szintű szórás ~3%, tehát az
átlagának standard hibája ≈ 0,34%. 80%-os erőhöz **~0,9%/call valódi élre**
lenne szükség — 1 napos horizonton irreálisan nagy.

| | |
|---|---|
| Kimutatható lett volna | hatalmas él (~0,9%/call) |
| Nem lett volna kimutatható | reális, szerény él (pl. 0,2%/call) |

Ehhez szerzőnként **több ezer call** kellene. A TradingView szerzők nem
posztolnak eleget ahhoz, hogy bárkiről el lehessen dönteni.

**A gyakorlati cél szempontjából ez ugyanaz, mint hogy nincs jel** — ha nem
tudod megmondani, kire fogadj, a képesség léte akadémikus kérdés.

**Populáció-definíció:** csak 2026-08-ban aktív szerzők. Deploymentre ez helyes
(aki abbahagyta, nem ad jövőbeli jelet), de a visszamérés „feltéve, hogy a
szerző ma is aktív" feltétel mellett értendő.

---

## Ami megmarad ebből a munkából

- **17 061 BTC call**, teljes szöveggel és **ground-truth iránnyal** — kész
  kalibrációs set bármilyen későbbi LLM-extraktorhoz. Az `s102` második indoka
  teljesült, függetlenül a hipotézis bukásától.
- **`btcusdt` OHLCV store** — 4,7M perces gyertya 2017-től, BTC quantitative
  munkára is használható.
- **Forrásfüggetlen `calls` / `call_returns` séma** — ha a téma valaha újra
  előkerül, az infrastruktúra áll.

## Reprodukálás

```bash
python -m data_handling.collectors.tradingview.backfill discover
python -m data_handling.collectors.tradingview.backfill authors
python -m data_handling.collectors.tradingview.normalize
python -m data_handling.sync_tables.sync_call_returns
python -m modeling.analyst_signal.score_authors
python -m modeling.analyst_signal.validate
```

Eredmények: `database/analyst_signal/analyst_signal.duckdb`
(`calls`, `call_returns`, `author_scores`, `score_params`, `oos_validation`).

A felderítés részletei: [tradingview_poc.md](tradingview_poc.md) (a `research/tradingview_poc/` törölve).

---

## Kód a git historyban

A kísérleti kód a repóból törlődik (epic_068, t894/t895). Törlés előtti állapot: commit `73f2156`, útvonalak: `src/modeling/analyst_signal/{score_authors,validate}.py`, `sync_tables/sync_call_returns.py`, `src/data_handling/collectors/tradingview/` (utóbbi marad).
