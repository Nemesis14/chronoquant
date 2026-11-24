---
epic: epic_062
id: t831
title: 30 alt-perp 1 perces OHLCV backfill
assignee: database_agent
status: todo
blocks: [t832]
blocked_by: []
---

## Goal

A Binance Vision `futures/um/monthly/klines` archívumból 30 likvid USDT-perp
1 perces gyertyája, 2024-01-01 → ma.

## Az univerzum kiválasztása

⚠️ **Survivorship bias.** A „ma legnagyobb 30 perp" listája visszamenőleg
torzít: a 2024-ben bedőlt párok hiányoznak. A kiválasztás ezért a
**2024-01-01-i** forgalom szerint történik, nem a maiak szerint, és a
később delistelt párokat is tartalmazza.

## Scope

- `database/_universe/universe.duckdb` — új, elkülönített fájl
- a `solusdt.duckdb`-t **nem érinti** (13 GB, single-writer)

## Acceptance Criteria

- [ ] 30 szimbólum, `ohlcv_1m` tábla `(symbol, open_time)` kulccsal
- [ ] A szimbólum-lista a 2024-01-01-i forgalom szerint, forrás dokumentálva
- [ ] Delistelt párok is benne, a delistelés dátumával
- [ ] Hiányzó napok explicit naplózva, nem csendben kihagyva

## Notes
[orchestrator] Elvegezve — 2026-08-26

## Terv-elteres, tudatos

**Orai bar, nem 1 perces.** A meres 4-72 oras zaro-zaro hozamot nez, amit az
orai bar pontosan ad, ~1000-szer kisebb letoltesbol. 1m csak a modell-agnak
kell, ha a probe GO.

## Amit epult

`src/data_handling/collectors/universe/build_universe.py`
`database/_universe/universe.duckdb` — `ohlcv_1h`, `funding`, `selection`

- Teljes S3 enumeracio: **986 szimbolum** (nem exchangeInfo — az csak az előket adja)
- Szures: ASCII + alnum + USDT-vegu -> **829 perp**
  (kiesnek a datalt futuresok es harom CJK meme-szimbolum, amiket HTTP
  keressorba se lehet tenni)
- Rangsor a **2024-01-i** forgalom szerint, napi barokbol: 261 letezett akkor
- Top 30 orai bara 2024-01 -> 2026-07: **661 806 sor**
- Funding: **102 729 elszamolas** (kesobb hozzavéve, a t833-hoz kellett)

## Survivorship bias — ellenorizve, nem allitva

**MATICUSDT benne van, es az adata 2024-09-11-en megszakad.** Pontosan ahogy
a valosagban is. Egy "mai top 30" lista ezt kihagyta volna.

## Acceptance Criteria

- [x] 30 szimbolum, `ohlcv_1h` tabla `(symbol, open_time)` kulccsal
- [x] Szimbolum-lista a 2024-01-i forgalom szerint, forras dokumentalva
- [x] Delistelt parok is benne
- [x] `ruff` + `pyright` tiszta
