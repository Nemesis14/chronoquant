---
epic: epic_061
id: t822
title: aggTrades archívum letöltés és 1 perces aggregálás
assignee: database_agent
status: todo
blocks: [t823]
blocked_by: []
---

## Goal

A Binance Vision `futures/um/daily/aggTrades/SOLUSDT` archívumból 1 perces
flow-aggregátum előállítása.

## ⚠️ Méret

10–15 GB tömörítve. **Nem tároljuk a nyers tickeket** — napi fájl letöltés →
1 perces aggregálás → a nyers fájl eldobása. A cél egy néhány száz MB-os
`aggtrades_1m` tábla, nem egy tick-archívum.

## Aggregálandó oszlopok (1 perces rács)

| oszlop | jelentés |
|---|---|
| `taker_buy_qty` / `taker_sell_qty` | a `is_buyer_maker` flag alapján |
| `taker_buy_notional` / `taker_sell_notional` | ár × mennyiség |
| `n_trades` | kötésszám |
| `qty_p50` / `qty_p90` / `qty_max` | kötésméret-eloszlás |
| `large_buy_notional` / `large_sell_notional` | a napi p95 fölötti kötések |

## Acceptance Criteria

- [ ] Letöltés újraindítható, napi granularitáson idempotens
- [ ] `aggtrades_1m` tábla, 2021-01-01 → ma
- [ ] A nyers zip-ek nem maradnak a lemezen
- [ ] Ellenőrzés: `taker_buy_qty + taker_sell_qty` ≈ az `ohlcv.volume` (±0,5%)

## Notes
