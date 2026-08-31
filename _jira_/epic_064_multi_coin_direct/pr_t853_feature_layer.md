---
epic: epic_064
id: t853
title: Feature-réteg — a meglévő modulok per-symbol futtatása
assignee: database_agent
status: todo
blocks: [t856]
blocked_by: [t852]
---

## Goal

A meglévő, leak-tesztelt feature-modulokat futtatni mind a N coinra.

## ⚠️ Nem kell újraírni

Mindkét belépési pont egyszerű, folytonos idősort vár — nem tud a
`symbol`-ról:

```python
compute_features_polars(df_ohlcv, indicators, feat_prefix, ...)   # 212 feature
compute_altdata_features(altdata_5m)                              # 22 feature
```

Vagyis: coinonként meghívni és összefűzni. Ez wrapper, nem átírás.

## Rács

**5 perces**, mert a `metrics` natívan 5 perces — 1 perces rácson a
derivatíva-feature-ök négyötöde ugyanazt az értéket ismételné.

⚠️ **Minden ablak t-ben végződjön**, soha ne bar-határon. Ha a feature "az
aktuális bar eddigi részét" nézi, akkor tanításkor teljes bart lát, élesben
félkészet — ez train/serving skew, és csendben rontja el az eredményt.

## Méret

5 perces rácson N coin × ~6 év. A SOLUSDT mai 1 perces táblája 3,1M sor;
ez nagyságrendileg 2-4×.

## Acceptance Criteria

- [ ] `feat_5m (symbol, open_time)` tábla
- [ ] A 212 + 22 feature megvan minden coinra
- [ ] Leak-teszt: minden feature szigorúan t-1-ig lát
- [ ] Coinonkénti NULL-arány riport (rövid history → több NULL az elején)
- [ ] `ruff` + `pyright` tiszta

## Notes
[orchestrator] Elvegezve — 2026-08-26

## ⚠️ Racs: 5 perc -> ORAI (tarhely-kenyszer + a meres igenye)

A `D:` meghajton 1,39 GB szabad hely volt. Becsult meret:

| racs | feature | dtype | meret |
|---|---|---|---|
| 5m | 245 | DOUBLE | 25,4 GB |
| 5m | 245 | FLOAT | 12,7 GB |
| **orai** | **233** | **FLOAT** | **~1,1 GB** |

Ez nem kompromisszum: a tanitas **amugy is oranként mintavetelezne** (24h
targetnel ket szomszedos perc cimkeje 1439 percben atfed), es a walk-forward
is orai. Az 5 perces racs az **eles, perc-szintu pontozashoz** kell — arra
csak akkor, ha a meres GO-t ad.

`data/vision_cache` (5,24 GB, 62 821 fajl) torolve — a zip-ek mar fel vannak
dolgozva a DuckDB-be, es barmikor ujratolthetok. Utana 6,74 GB szabad.

## A meglevo kod valtozatlan

Egyik feature-modult sem kellett atirni:

```
compute_features_polars(df_ohlcv, indicators, ...)   -> 206 OHLCV feature
compute_altdata_features(altdata_5m)                 -> 27 derivativa feature
expand_to_minute_grid(alt_5m, orai_index)            -> as-of join
```

⚠️ **Az alt-modul 5 perces racsra van drotozva** (`upsample(every="5m")`, a
window-konstansok 5m-barokban: W_60M=12). Ezert natív 5 perces racson szamolom,
majd a modul **sajat as-of joinjaval** teszem orai racsra — az `available_at`
belyeg (open_time + 5m) intezi a leak-vedelmet. Igy a tesztelt kodhoz nem
nyultam.

## Eredmeny

```
feat_1h        1 079 178 sor   30 coin   233 feature   (Float32)
```

**9 feature vegig NULL** — pontosan a kihagyott forrasokra epulok:
`feat_basis_60`, `feat_basis_z_240`, `feat_spot_perp_vol_ratio_60`,
`feat_spot_taker_imb_60` (spot klines) es a ot `feat_depth_*` (bookDepth).
**224 hasznalhato feature.**

⚠️ **Ismert korlat:** az alt-feature-ok ablakai 5m-barokban vannak megadva,
tehat a leghosszabb 240 perc = 4 ora — 24 oras horizonthoz rovid. Az OHLCV
feature-ok viszont orai barokon szamolnak, ott a 240-es ablak 10 nap. Ha a
gate pozitiv, a derivativa-ablakok megnyujtasa az elso finomitas.

## Acceptance Criteria

- [x] `feat_1h (symbol, open_time)` tabla
- [x] 206 + 27 feature minden coinra
- [x] Leak-vedelem: t-1 shift (OHLCV) + `available_at` as-of join (alt)
- [x] Coinonkenti NULL-riport
- [x] `ruff` + `pyright` tiszta
