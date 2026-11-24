## A stratégia működése

A stratégia a modell nyers predikciós pontszámát három offline származtatott elemmel kereskedési döntéssé alakítja:

1. **Rank kalibráció** - a nyers `short_pred` pontszámot a kalibrációs periódusból épített lookup (`rank_lookup_short.parquet`) rank-percentilisre (`short_pct`, 0-1) képezi. Alacsony `short_pct` = a legerősebb short jel. Lásd: `_doc_/methodology_doc/6100_strategy_calibration.md`.
2. **Belépési szabály** - egy bár short belépőt ad, ha `(1 - short_pct) >= 0.99`, azaz `short_pct <= 0.01` (a legszélsőségesebb short jelek alsó 1.0%-a). A belépés a jelző bár close árán történik. Lásd: `_doc_/methodology_doc/6300_strategy_grid_search.md`.
3. **Kilépési szabály** - amelyik előbb teljesül: **Take Profit**, ha a következő bár low értéke eléri az `entry_close x exp(bucket_median_mfe)` szintet (a TP log return -0.006387, azaz -0.6387%, vagyis az ár alá esik), vagy **Timeout**: 60 bár (60 perc) után a pozíció a záró close áron zár. Stop loss: `2.0x_tp`.

A TP szint (`bucket_median_mfe = -0.006387`) a felső kalibrációs bucket mintáinak mediánja. Teljes stratégiatervezés: `_doc_/methodology_doc/6000_strategy.md`.

## Stratégia összefoglaló

{{table:strategy_summary}}

## Heti trade összesítő

Zöld = TP találat, sárga = pozitív timeout, piros = negatív timeout. A $ értékek 1,000 USDT pozícióméretre vonatkoznak.

{{table:weekly_summary}}

{{chart:equity}}

## Validációs periódus - heti ábrák

Minden héthez (legalább egy trade) egy ábra: felül a close és a belépések, alul a rank-percentilis a belépési küszöbbel.

### 2026-01-26 - 2026-02-01 (3 trade)

{{chart:week_2026_01_26}}

### 2026-02-02 - 2026-02-08 (50 trade)

{{chart:week_2026_02_02}}

### 2026-02-09 - 2026-02-15 (1 trade)

{{chart:week_2026_02_09}}

### 2026-02-16 - 2026-02-22 (2 trade)

{{chart:week_2026_02_16}}

### 2026-02-23 - 2026-03-01 (21 trade)

{{chart:week_2026_02_23}}

### 2026-03-02 - 2026-03-08 (1 trade)

{{chart:week_2026_03_02}}

### 2026-03-23 - 2026-03-29 (1 trade)

{{chart:week_2026_03_23}}

### 2026-05-04 - 2026-05-10 (3 trade)

{{chart:week_2026_05_04}}

### 2026-05-18 - 2026-05-24 (22 trade)

{{chart:week_2026_05_18}}

### 2026-05-25 - 2026-05-31 (4 trade)

{{chart:week_2026_05_25}}

### 2026-06-01 - 2026-06-07 (94 trade)

{{chart:week_2026_06_01}}

### 2026-06-08 - 2026-06-14 (18 trade)

{{chart:week_2026_06_08}}

### 2026-06-15 - 2026-06-21 (3 trade)

{{chart:week_2026_06_15}}

### 2026-06-22 - 2026-06-28 (9 trade)

{{chart:week_2026_06_22}}

## Értelmezés

- A validációs periódusban **232** trade volt, ebből 130 TP (56.0% TP arány), a nettó eredmény +144 USDT 1,000 USDT pozícióméreten.

- A timeout kilépések (102 db) döntik el az eredményt: a TP arány magas, a stratégia többnyire eléri a célt.
- Az artifact összetett hozama 13.90%, win rate 0.634; a heti táblában a negatív hetek száma mutatja a hozam egyenletességét.
