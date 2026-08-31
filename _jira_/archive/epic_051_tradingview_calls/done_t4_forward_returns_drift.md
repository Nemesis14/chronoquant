---
epic: epic_051
id: t4
title: Forward return join + drift-korrekció (call_returns tábla)
assignee: —          # főszálon fut, agent nélkül (a t1-t3 folytatása)
status: done
blocks: [t5]
blocked_by: []
---

## Goal

A `calls` tábla összekötése a BTCUSDT spot OHLCV store-ral, és a
**drift-korrigált** forward returnök kiszámítása minden (call, horizont) párra.

## Scope

```
config/assets.json                                  ← btcusdt asset (spot, 1m)
database/btcusdt/btcusdt.duckdb                     ← 4 729 212 gyertya
src/data_handling/store/analyst_signal_store.py     ← migráció v2: call_returns
src/data_handling/sync_tables/sync_call_returns.py  ← a számítás
```

Horizontok: **60, 240, 1440, 4320, 10080 perc** (1h, 4h, 1d, 3d, 7d).

## Spec-módosítás az eredeti s102-höz képest

Az `s102` T4-e „BTC-béta korrekciót" ír — de az a SOL/ETH scope-ra volt szabva,
ahol a BTC a külső referencia. **BTC-only mellett nincs külső referencia: maga a
BTC a saját benchmarkja**, tehát a korrekció a BTC saját driftje ellen megy
(excess return vs. buy-and-hold).

## Miért kötelező

A `t2` review kimutatta: a callok **61,5%-a long**, minden évben 50% felett.
BTC-emelkedésben a nyers hozam a **long-torzítást rangsorolná, nem a képességet**.

A valódi mértéke ezen az adaton, a tényleges call-időpontokban:

| h | drift μ̂ | long nyers | short nyers | különbség |
|---|---:|---:|---:|---:|
| 1h | 0,005% | +0,006% | −0,005% | 0,01 pp |
| 1d | 0,056% | +0,039% | −0,070% | 0,11 pp |
| **7d** | **0,404%** | **+0,321%** | **−0,530%** | **0,85 pp** |

Vagyis rövid horizonton elhanyagolható, **3–7 napon viszont érdemi**: két
nulla képességű szerző között 0,85 százalékpont különbség pusztán az irányból.
És mivel a torzítás nem csökken a callok számával (a zaj igen), **pont a
legtermékenyebb szerzőknél harap a legjobban** — akik a rangsor tetejére
kerülnének.

## Két független drift-becslés

**`drift_bucket` (elsődleges)** — keresztmetszeti: az azonos naptári hónapba eső
callok átlagos piaci hozama az adott horizonton. Kis szórású, és **kondicionál
az időszakra**, amit egy globális átlag nem tud. Mindenkire ugyanaz megy, tehát
szerző-szintű leaket nem okoz. Ritka bucketnél (< 20 call) éves átlagra esik vissza.

**`drift_trailing` (robusztusság)** — szigorúan leak-mentes: a megelőző 365 nap
log-árváltozásából implikált drift, horizontra skálázva. Zajosabb, de kizárólag
`t` előtti adatot használ.

**Egyeznek:** korreláció **0,99 (1h) … 0,88 (7d)**, és ugyanazt a képet adják.

## Volatilitás-normalizálás

A BTC volatilitása korszakonként nagyságrendekkel tér el, ezért normalizálás
nélkül egy szerző átlagát a turbulens időszakba eső callok uralnák. A `z` oszlop
a korrigált hozamot elosztja a megelőző 30 nap realizált volatilitásával,
horizontra skálázva — így a callok időben összemérhetők.

## Acceptance Criteria

- [x] `btcusdt` asset (spot, 1m) + OHLCV sync 2017-08-17-től
- [x] `call_returns` tábla migrációval, PK `(call_id, horizon_min)`
- [x] Tűréshatáros ASOF illesztés (a store-ban 33 órás Binance-lyukak vannak)
- [x] `drift_bucket` + `drift_trailing`, mindkettő tárolva
- [x] `vol` és `z` (volatilitás-normalizált excess)
- [x] Csonka ablak → NULL, nem nulla
- [x] **Sorvesztés-őr**: minden call minden horizonton megjelenik
- [x] `ruff check` + `pyright` tiszta

## Notes

### Egy elkapott hiba: a néma sorvesztés

Az első futás **16 917 sort adott 17 061 helyett — 144 call némán eltűnt.**
Ok: a DuckDB `ASOF JOIN` inner join, és a trailing-drift CTE 365 nappal korábbi
árat kér, így **minden 2018-08-17 előtti call kiesett** — pont a legrégebbiek.

Javítás: mind a négy join `ASOF LEFT JOIN`, plusz egy **őrszem**, ami kivételt
dob, ha a sorszám nem egyezik a `calls` táblával. Ez a hibaosztály némán
torzította volna a mintát a teljes árlefedettségű időszakok felé.

### NULL-mintázat (mind magyarázott)

```
r_mkt            34 (1h) → 203 (7d)   a legfrissebb calloknak nincs 7 napnyi jövője
drift_trailing   144                  a 2018-08-17 elottiek — most NULL, nem eltunt
vol              28                   a legkorabbiak, 30 napnyi vol-elozmeny nelkul
drift_bucket     12                   olyan bucket, ahol minden r_mkt NULL
```

### Előzetes megfigyelés — óvatosan

Korrekció után **mindkét irány enyhén negatív** (7d: long −0,15%, short −0,23%),
és ez a trailing becsléssel is így jön ki. Vagyis az **átlagos** call
alulteljesíti a buy-and-holdot.

**Ez nem az epic válasza.** A kérdés nem az átlag, hanem hogy *egyes szerzők*
perzisztensen verik-e — ez a T5 dolga. Az viszont már most látszik, hogy a
mezőny egészétől nem kell csodát várni.

---

### Eredmény — 2026-08-20

```
call_returns : 85 305 sor  (17 061 call x 5 horizont)
illesztes    : 99,8% (1h..3d), 98,8% (7d)
|z| atlag    : 0,66 - 0,71
```

OHLCV: **4 729 212 gyertya, 2017-08-17 .. 2026-08-20, 0,18% hiány**
(mind Binance karbantartás; 2022-től 100%).
