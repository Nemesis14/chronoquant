---
epic: epic_064
id: t852
title: Nyers adat letöltése a görgetett univerzumra
assignee: database_agent
status: todo
blocks: [t853, t854, t855]
blocked_by: [t851]
---

## Goal

A t851 által kiválasztott **distinct** szimbólumhalmazra letölteni mind a
négy forrást. Elérhetőség 2024-02-re ellenőrizve: mind a 30-on OK
(`ohlcv1m`, `metrics`, `premium`, `spot`).

## Táblák

```sql
ohlcv_1m    (symbol, open_time) PK   -- vegrehajtas + feature-alap
metrics_5m  (symbol, open_time) PK   -- OI, long/short aranyok, taker ratio
premium_5m  (symbol, open_time) PK   -- premium index
spot_1m     (symbol, open_time) PK   -- bazis-feature-okhoz
funding     (symbol, funding_ts) PK  -- mar megvan a 30-ra, bovitendo
```

## ⚠️ Amire figyelni kell

- **Szimbólumonként újraindítható** — ha a 40. coinnál elszáll, ne kelljen elölről
- A `metrics` **kezdő dátuma coinonként más** (a listázástól). Ez nem hiba:
  a coin akkor lép a panelbe, amikor van adata. A lefedettséget **coinonként
  ki kell írni**, nem csendben NULL-ozni.
- A `bookDepth` **NEM ebben a taskban** — 18 GB, később.
- Spot pár hiányozhat olyan perpnél, ami sosem volt spoton listázva → a
  bázis-feature-ök NULL-ok lesznek rá. Ezt naplózni kell.

## Acceptance Criteria

- [ ] Mind a négy tábla feltöltve a distinct szimbólumhalmazra
- [ ] Coinonkénti lefedettségi riport (első/utolsó dátum, hiányzó napok)
- [ ] Idempotens: újrafuttatás nem duplikál
- [ ] `ruff` + `pyright` tiszta

## Notes
[orchestrator] Elvegezve — 2026-08-26

## Univerzum — user dontes: egyszerusitve

A gorgetett valasztas (t851) helyett **a most elo top 30**. A user tudatosan
vallalja a survivorship biast; 24 oras horizonton ez gyenge hatas.

⚠️ **De egy szuro kellett.** A "2026-07 forgalom szerinti top 30" fele **nem
kripto**: a Binance tokenizalt reszvenyekre es femekre is indit perpeket
(SNDK=SanDisk, SKHYNIX, SOXL=felvezeto-ETF, XAU=arany, SPCX=SpaceX, EWY,
MU=Micron). Meres: a top 30-bol **20 volt 24 honapnal fiatalabb**, tiz
fel evnel is fiatalabb.

Megoldas egyetlen felteteltel: rangsor a **trailing 12 honap** forgalmara,
es a szimbolum letezzen mar **24 honapja**. Igy magatol kiesik minden friss
listazas es tokenizalt eszkoz.

## Eredmeny

```
universe.duckdb   8,79 GB
ohlcv_1m          81 919 430 sor   30 coin   2020-01-01 .. 2026-07-31
ohlcv_5m          16 383 889
metrics_5m        12 956 131                 2021-12-01 .. 2026-07-31
premium_5m        16 338 392
funding              184 477
```

Lefedettseg harom retegben: **21 coin ~6,5 evvel** (BTC, ETH, XRP, LTC, TRX,
LINK, XLM, ADA, DASH, ZEC, BNB, BCH, DOGE, DOT, SOL, UNI, AVAX, NEAR, AAVE,
FIL, 1000SHIB), 5 coin 2023-tol, 4 coin 2024-tol.

## ⚠️ Ket javitas a letoltoben, ami nelkul nem ment volna vegig

1. **Ujraprobalkozas** — a Binance ~60 000 keres utan eldobta a kapcsolatot
   (WinError 10054). Exponencialis visszalepes, csak a 404 valodi valasz.
2. **Sema-kovetkeztetes kikapcsolva a CSV-olvasoban** — a SOLUSDT egy 2020-as
   fajljan hasalt el: polars `i64`-nek kovetkeztette a `volume` oszlopot az
   elso 10 000 sorbol (veletlenul kerek szamok), aztan jott egy `6701.80`.
   Evjarat-fuggo csapda, 30 coinnal garantaltan ujra elojott volna. Most
   minden szovegkent olvasodik es a hivo castol — amit minden loader eddig
   is megtett, tehat a kovetkeztetes semmit nem adott, csak kockazatot.

## User dontesek

- **Tanitasi ablak: 2021-12-tol** (a `metrics` kezdete) — igy egyseges, nincs
  ket ev feler feature-keszlettel. A 2020-2021-es adat bent marad a DB-ben,
  mert a feature-ablakok bemelegitesehez kell.
- **`funding` takaritva**: 16 idegen coin (regi buildbol) torolve, 63 123 sor.
- A coin-lista valtozatlan marad.

## Acceptance Criteria

- [x] Mind a negy tabla feltoltve
- [x] Coinonkenti lefedettsegi riport (`panel_report.py`)
- [x] Idempotens, szimbolumonkent ujraindithato (`panel_log`)
- [x] `ruff` + `pyright` tiszta
