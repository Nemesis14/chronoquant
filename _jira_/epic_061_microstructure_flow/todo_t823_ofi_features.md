---
epic: epic_061
id: t823
title: Order flow imbalance feature-réteg
assignee: database_agent
status: todo
blocks: [t824]
blocked_by: [t822]
---

## Goal

Az `aggtrades_1m`-ből olyan változókat képezni, amik **nem** ár-származtatottak.

## A jelöltek

| név | képlet |
|---|---|
| `feat_ofi_15` / `feat_ofi_60` | (buy_notional − sell_notional) / össz, gördülő |
| `feat_ofi_z_240` | ugyanaz, trailing z-score |
| `feat_large_ofi_60` | csak a p95 fölötti kötésekre |
| `feat_trade_size_skew_60` | qty_p90 / qty_p50 |
| `feat_trade_intensity_z_240` | n_trades trailing z-score |
| `feat_ofi_price_div_60` | OFI és a 60 perces hozam eltérése (abszorpció) |

⚠️ **t−1 lag kötelező**, mint minden feature-nél. Az utolsó képlet a
legérdekesebb: nagy vásárlói flow **ár-elmozdulás nélkül** azt jelenti,
hogy valaki elnyeli — ez történetileg fordulat-jel.

## Acceptance Criteria

- [ ] A 6 feature a `feat_altdata`-ban, majd a `quant_train`-ben
- [ ] Leak-teszt: minden feature szigorúan t−1-ig lát
- [ ] Korreláció a meglévő 235-tel — ami > 0,95, az kiesik
- [ ] `ruff check` + `pyright` tiszta

## Notes
