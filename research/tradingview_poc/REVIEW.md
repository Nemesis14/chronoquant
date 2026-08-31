# TradingView backfill — adatminőségi review

> `epic_051 / t2` · lefutva 2026-08-17 · **1913 kérés, 191 perc, 397 MB, nulla rate-limit**
> Újrafuttatható: `python -m data_handling.collectors.tradingview.review`
> Gépi változat: [review_report.json](review_report.json)

---

## Összefoglaló ítélet

**Az adat elbírja a mérést.** 281 szerző van legalább 10 irányos BTC callal,
146 szerző legalább 30-cal — ez elég populáció a szerző-skill rangsorhoz.
A dátum és az irány 100%-ban strukturált, a leak-kritikus `created_at`
minden rekordon megvan.

**Egy dolog viszont eldönti a mérés érvényességét, és nem opcionális:
a long-torzítás korrekciója.** Részletek a „Kockázatok" szakaszban.

---

## 1. Mennyiség

| | |
|---|---:|
| Idea összesen (minden szimbólum) | 144 966 |
| Ebből BTC | **30 235** (20,9%) |
| Használható BTC call (long/short) | **18 236** (60,3% a BTC-ből) |
| Felderített szerző | 741 |
| — van BTC ideája | 740 |
| — van **irányos** BTC callja | **638** |
| — csak irány nélküli BTC | 102 |

A 144 966-os összesen azért ilyen nagy, mert a `by={username}` lekérdezés a
szerző *összes* ideáját visszaadja, nem csak a BTC-t. A raw réteg szándékosan
teljes marad — a BTC-szűrés a T3 dolga.

## 2. A mérhető populáció ← ez a fő kérdés

| küszöb | szerző | összes call |
|---|---:|---:|
| ≥ 10 call | **281** | 17 016 |
| ≥ 20 call | 180 | 15 617 |
| ≥ 30 call | **146** | 14 775 |
| ≥ 50 call | 94 | 12 842 |
| ≥ 100 call | 47 | 9 315 |

Eloszlás: `min=1 · p25=2 · medián=7 · p75=26 · p90=85 · max=716`

A ≥10-es szerzők history-hossza: **medián 2 év, max 11 év**.

**Gyakorlati küszöb a T5-höz: ≥ 30 call → 146 szerző.** A rolling
out-of-sample rangsorhoz a callokat tanító és teszt ablakra kell vágni;
10 call ehhez kevés, 30 már kényelmes. A `s102` shrinkage-előírása így is
kötelező marad — a medián 7 call miatt a hosszú farokban rengeteg a zaj.

Legaktívabb szerzők: `BitCoinGuide` 716, `Blayno_MTOPS` 499,
`wolf_king888` 445, `pejman_zwin` 370, `EXCAVO` 329.

## 3. Adatminőség — mező-kitöltöttség (n = 144 966)

| mező | kitöltöttség | |
|---|---:|---|
| `created_at` | **100,0%** | ✅ leak-kritikus, UTC, másodperc |
| `date_timestamp` | **100,0%** | ✅ ugyanaz unix intként |
| `symbol.direction` | **100,0%** | ✅ mező mindig jelen |
| `symbol.interval` | **100,0%** | ✅ szerző saját timeframe-je |
| `user.id` | **100,0%** | ✅ stabil numerikus szerző-azonosító |
| `likes_count` | **100,0%** | ✅ confidence proxy |
| `description` | 100,0% | 15 hiányzó a 144 966-ból |
| `updated_at` | 29,6% | ~30% szerkesztve — de `created_at` megvan |

**Nincs hiányzó dátum és nincs hiányzó szerző-azonosító.** Ez volt a `t1`
két legnagyobb kockázata, mindkettő megszűnt.

## 4. Irány-eloszlás (BTC ideák)

| | n | % |
|---|---:|---:|
| long | 11 220 | 37,1% |
| **none** | 11 027 | 36,5% |
| short | 7 016 | 23,2% |
| education | 972 | 3,2% |

A 36,5% irány nélküli a legnagyobb veszteségforrás. Ezek elemzések irány
megjelölése nélkül — nem hiba, egyszerűen nem call.

## 5. Időbeli lefedettség (18 236 használható call)

```
2015       6
2016       8
2017      98
2018     360  ###
2019     502  ####
2020     576  #####
2021   1 088  ##########
2022   1 193  ###########
2023   1 753  ################
2024   2 917  ###########################
2025   4 309  #######################################
2026   5 426  ##################################################
```

Tartomány: **2015-03-04 .. 2026-08-17**.

A sűrűség viszont erősen a jelen felé tolódik: a callok **63%-a 2024 után**
keletkezett. Ennek oka részben a platform növekedése, részben az, hogy a
felderítés a *jelenleg aktív* szerzőket találja meg (ez szándékos, lásd
`epic.md` BTC-first). **Gyakorlati mérési ablak: 2021+, sűrűn 2023+.**

## 6. A szerző által megadott timeframe

| interval | n | |
|---|---:|---|
| `240` | 4 066 | 4 óra |
| `1D` | 3 380 | 1 nap |
| `60` | 3 325 | 1 óra |
| `1W` | 1 531 | 1 hét |
| `30` | 1 470 | 30 perc |
| `15` | 966 | 15 perc |
| `120` | 899 | 2 óra |

Ez **nem a mi feltételezésünk, hanem a szerző saját horizontja.** Ráfekszik
az `s102` T4 rácsára (`h ∈ {1h, 4h, 1d, 3d, 7d}`), és lehetővé teszi, hogy
minden callt a szerző *saját* horizontján is értékeljünk, ne csak fix rácson.

## 7. Tőzsde-névtér (BTC ideák)

`BINANCE` 12 460 · `BITSTAMP` 4 500 · `COINBASE` 3 367 · `BYBIT` 2 618 ·
`CRYPTO` 1 351 · `INDEX` 730 · `BITFINEX` 618

**A domináns névtér a BINANCE**, és a `sync_ohlcv.py` is Binance-ből húz —
az árillesztés tiszta. BTC-nél a tőzsdék közti árkülönbség elhanyagolható,
tehát a többi névtér is ugyanarra a sorozatra köthető.

## 8. Ami NINCS az adatban

- **belépő ár (entry)** — nincs mező az API-ban
- **célár (target) / stop** — nincs mező az API-ban
- **pozícióméret / R-multiple** — nincs

Igazolva a `t1`-ben: 0/5 detail oldal tartalmazott pozíció-eszközt, és az
API-válaszban egyetlen ár-jellegű kulcs sincs. Az árszintek szabad szövegben
és a chart képen élnek.

**Következmény:** a mérés `direction` + `ts_utc` alapú — pontosan az, amit a
`plan.md` `call` absztrakciója feltételez. Nem mérhető viszont az R-multiple,
a saját stop/target teljesülése, és az, hogy a szerző tartotta-e a tervét.

---

## Kockázatok

### 🔴 Long-torzítás — ez dönti el a mérés érvényességét

A callok **61,5%-a long**, és ez minden évben 50% felett van:

| év | long | short | long arány |
|---|---:|---:|---:|
| 2018 | 263 | 97 | **73,1%** |
| 2019 | 408 | 94 | **81,3%** |
| 2020 | 390 | 186 | 67,7% |
| 2021 | 758 | 330 | 69,7% |
| 2022 | 784 | 409 | 65,7% |
| 2023 | 1 058 | 695 | 60,4% |
| 2024 | 1 876 | 1 041 | 64,3% |
| 2025 | 2 681 | 1 628 | 62,2% |
| 2026 | 2 933 | 2 493 | 54,1% |

Emelkedő BTC-piacon a nyers „bejött-e a call" mérés **a leginkább
long-torzított szerzőt rangsorolja elsőnek, nem a legjobbat.** Egy szerző,
aki mindig longot mond, egy bull évben tökéletesnek látszik.

**Kezelés (T4 spec-módosítás):** BTC-only scope mellett a „BTC-béta
korrekció" jelentése megváltozik. Nem másik asset ellen kell korrigálni,
hanem **a BTC saját drift-je ellen**: a forward returnből le kell vonni az
azonos horizonton mért piaci hozamot. A mért mennyiség így *excess return
vs. buy-and-hold*, nem nyers hozam. Enélkül a T7 döntés értelmetlen.

### 🟡 Időbeli egyenetlenség
A callok 63%-a 2024 utáni. A rolling out-of-sample mérésnek reálisan
**3-4 év** áll rendelkezésére, nem 11.

### 🟡 Szerkesztett ideák
`updated_at` 29,6%-ban kitöltött. A `created_at` mindenhol megvan, tehát a
`t1`-ben azonosított lookahead leak elkerülhető — de a T3-nak **kifejezetten
a `created_at`-et kell használnia**, soha nem az `updated_at`-et.

### 🟡 Populáció-definíció
Csak 2026-08-ban aktív szerzők. Deploymentre helyes (user döntés), de a
visszamérés így „feltéve, hogy a szerző ma is aktív" feltétel mellett
értendő — a T7 riportban ki kell mondani.

---

## Következő lépések

- **T3** — normalizálás `calls` táblává. Kritikus: `ts_utc = created_at`,
  `direction` a `symbol.direction`-ból (1=long, 2=short), BTC-szűrés,
  `is_education` kizárás, idempotens `call_id`.
- **T4 előfeltétel** — `btcusdt` asset felvétele a `config/assets.json`-ba
  és OHLCV sync. Javaslat `market: "spot"` (Binance spot 2017-08-tól,
  a futures csak 2019-09-től).
- **T4 spec-módosítás** — drift-korrekció, lásd fent.
