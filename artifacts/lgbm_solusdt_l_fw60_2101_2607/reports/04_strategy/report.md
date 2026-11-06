## A stratégia működése

A stratégia a modell nyers predikciós pontszámát három offline származtatott elemmel kereskedési döntéssé alakítja:

1. **Rank kalibráció** - a nyers `long_pred` pontszámot a kalibrációs periódusból épített lookup (`rank_lookup_long.parquet`) rank-percentilisre (`long_pct`, 0-1) képezi. Magas `long_pct` = a legerősebb long jel. Lásd: `_doc_/methodology_doc/6100_strategy_calibration.md`.
2. **Belépési szabály** - egy bár long belépőt ad, ha `long_pct >= 0.9` (a kalibrációs eloszlás felső 10.0%-a). A belépés a jelző bár close árán történik. Lásd: `_doc_/methodology_doc/6300_strategy_grid_search.md`.
3. **Kilépési szabály** - amelyik előbb teljesül: **Take Profit**, ha a következő bár high értéke eléri az `entry_close x exp(bucket_median_mfe)` szintet (a TP log return +0.006572, azaz +0.6572%, vagyis az ár fölé emelkedik), vagy **Timeout**: 60 bár (60 perc) után a pozíció a záró close áron zár. Stop loss: `0.5x_tp`.

A TP szint (`bucket_median_mfe = 0.006572`) a felső kalibrációs bucket mintáinak mediánja. Teljes stratégiatervezés: `_doc_/methodology_doc/6000_strategy.md`.

## Stratégia összefoglaló

{{table:strategy_summary}}

## Heti trade összesítő

Zöld = TP találat, sárga = pozitív timeout, piros = negatív timeout. A $ értékek 1,000 USDT pozícióméretre vonatkoznak.

{{table:weekly_summary}}

{{chart:equity}}

## Validációs periódus - heti ábrák

Minden héthez (legalább egy trade) egy ábra: felül a close és a belépések, alul a rank-percentilis a belépési küszöbbel.

### 2026-01-26 - 2026-02-01 (70 trade)

{{chart:week_2026_01_26}}

### 2026-02-02 - 2026-02-08 (632 trade)

{{chart:week_2026_02_02}}

### 2026-02-09 - 2026-02-15 (174 trade)

{{chart:week_2026_02_09}}

### 2026-02-16 - 2026-02-22 (86 trade)

{{chart:week_2026_02_16}}

### 2026-02-23 - 2026-03-01 (202 trade)

{{chart:week_2026_02_23}}

### 2026-03-02 - 2026-03-08 (184 trade)

{{chart:week_2026_03_02}}

### 2026-03-09 - 2026-03-15 (113 trade)

{{chart:week_2026_03_09}}

### 2026-03-16 - 2026-03-22 (68 trade)

{{chart:week_2026_03_16}}

### 2026-03-23 - 2026-03-29 (53 trade)

{{chart:week_2026_03_23}}

### 2026-03-30 - 2026-04-05 (66 trade)

{{chart:week_2026_03_30}}

### 2026-04-06 - 2026-04-12 (43 trade)

{{chart:week_2026_04_06}}

### 2026-04-13 - 2026-04-19 (49 trade)

{{chart:week_2026_04_13}}

### 2026-04-20 - 2026-04-26 (6 trade)

{{chart:week_2026_04_20}}

### 2026-04-27 - 2026-05-03 (4 trade)

{{chart:week_2026_04_27}}

### 2026-05-04 - 2026-05-10 (14 trade)

{{chart:week_2026_05_04}}

### 2026-05-11 - 2026-05-17 (19 trade)

{{chart:week_2026_05_11}}

### 2026-05-18 - 2026-05-24 (24 trade)

{{chart:week_2026_05_18}}

### 2026-05-25 - 2026-05-31 (16 trade)

{{chart:week_2026_05_25}}

### 2026-06-01 - 2026-06-07 (383 trade)

{{chart:week_2026_06_01}}

### 2026-06-08 - 2026-06-14 (98 trade)

{{chart:week_2026_06_08}}

### 2026-06-15 - 2026-06-21 (70 trade)

{{chart:week_2026_06_15}}

### 2026-06-22 - 2026-06-28 (165 trade)

{{chart:week_2026_06_22}}

### 2026-06-29 - 2026-07-05 (98 trade)

{{chart:week_2026_06_29}}

### 2026-07-06 - 2026-07-12 (25 trade)

{{chart:week_2026_07_06}}

### 2026-07-13 - 2026-07-19 (11 trade)

{{chart:week_2026_07_13}}

### 2026-07-20 - 2026-07-26 (1 trade)

{{chart:week_2026_07_20}}

### 2026-07-27 - 2026-08-02 (8 trade)

{{chart:week_2026_07_27}}

## Értelmezés

- A validációs periódusban **2682** trade volt, ebből 845 TP (31.5% TP arány), a nettó eredmény +519 USDT 1,000 USDT pozícióméreten.

- A timeout kilépések (1837 db) döntik el az eredményt: a TP arány mérsékelt, a timeoutok súlya jelentős.
- Az artifact összetett hozama 60.02%, win rate 0.363; a heti táblában a negatív hetek száma mutatja a hozam egyenletességét.
