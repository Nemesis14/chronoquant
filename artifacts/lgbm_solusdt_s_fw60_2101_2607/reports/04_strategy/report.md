## A stratégia működése

A stratégia a modell nyers predikciós pontszámát három offline származtatott elemmel kereskedési döntéssé alakítja:

1. **Rank kalibráció** - a nyers `short_pred` pontszámot a kalibrációs periódusból épített lookup (`rank_lookup_short.parquet`) rank-percentilisre (`short_pct`, 0-1) képezi. Alacsony `short_pct` = a legerősebb short jel. Lásd: `_doc_/methodology_doc/6100_strategy_calibration.md`.
2. **Belépési szabály** - egy bár short belépőt ad, ha `(1 - short_pct) >= 0.92`, azaz `short_pct <= 0.08` (a legszélsőségesebb short jelek alsó 8.0%-a). A belépés a jelző bár close árán történik. Lásd: `_doc_/methodology_doc/6300_strategy_grid_search.md`.
3. **Kilépési szabály** - amelyik előbb teljesül: **Take Profit**, ha a következő bár low értéke eléri az `entry_close x exp(bucket_median_mfe)` szintet (a TP log return -0.006620, azaz -0.6620%, vagyis az ár alá esik), vagy **Timeout**: 60 bár (60 perc) után a pozíció a záró close áron zár. Stop loss: `0.5x_tp`.

A TP szint (`bucket_median_mfe = -0.006620`) a felső kalibrációs bucket mintáinak mediánja. Teljes stratégiatervezés: `_doc_/methodology_doc/6000_strategy.md`.

## Stratégia összefoglaló

{{table:strategy_summary}}

## Heti trade összesítő

Zöld = TP találat, sárga = pozitív timeout, piros = negatív timeout. A $ értékek 1,000 USDT pozícióméretre vonatkoznak.

{{table:weekly_summary}}

{{chart:equity}}

## Validációs periódus - heti ábrák

Minden héthez (legalább egy trade) egy ábra: felül a close és a belépések, alul a rank-percentilis a belépési küszöbbel.

_Az élő adatbázis nem érhető el (zárolt) - a heti ábrák kimaradtak._

## Értelmezés

- A validációs periódusban **2988** trade volt, ebből 999 TP (33.4% TP arány), a nettó eredmény +201 USDT 1,000 USDT pozícióméreten.

- A timeout kilépések (1989 db) döntik el az eredményt: a TP arány mérsékelt, a timeoutok súlya jelentős.
- Az artifact összetett hozama 18.29%, win rate 0.341; a heti táblában a negatív hetek száma mutatja a hozam egyenletességét.
