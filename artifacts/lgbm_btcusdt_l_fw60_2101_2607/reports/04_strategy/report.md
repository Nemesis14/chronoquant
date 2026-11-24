## A stratégia működése

A stratégia a modell nyers predikciós pontszámát három offline származtatott elemmel kereskedési döntéssé alakítja:

1. **Rank kalibráció** - a nyers `long_pred` pontszámot a kalibrációs periódusból épített lookup (`rank_lookup_long.parquet`) rank-percentilisre (`long_pct`, 0-1) képezi. Magas `long_pct` = a legerősebb long jel. Lásd: `_doc_/methodology_doc/6100_strategy_calibration.md`.
2. **Belépési szabály** - egy bár long belépőt ad, ha `long_pct >= 0.99` (a kalibrációs eloszlás felső 1.0%-a). A belépés a jelző bár close árán történik. Lásd: `_doc_/methodology_doc/6300_strategy_grid_search.md`.
3. **Kilépési szabály** - amelyik előbb teljesül: **Take Profit**, ha a következő bár high értéke eléri az `entry_close x exp(bucket_median_mfe)` szintet (a TP log return +0.004732, azaz +0.4732%, vagyis az ár fölé emelkedik), vagy **Timeout**: 60 bár (60 perc) után a pozíció a záró close áron zár. Stop loss: `none`.

A TP szint (`bucket_median_mfe = 0.004732`) a felső kalibrációs bucket mintáinak mediánja. Teljes stratégiatervezés: `_doc_/methodology_doc/6000_strategy.md`.

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

- A validációs periódusban **203** trade volt, ebből 144 TP (70.9% TP arány), a nettó eredmény +152 USDT 1,000 USDT pozícióméreten.

- A timeout kilépések (59 db) döntik el az eredményt: a TP arány magas, a stratégia többnyire eléri a célt.
- Az artifact összetett hozama 15.52%, win rate 0.759; a heti táblában a negatív hetek száma mutatja a hozam egyenletességét.
