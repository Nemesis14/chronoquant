## Cél

Az `5300_impulse_scout.md` v1 szerinti, több-asset eseményadatkészlet: ugyanaz az esemény és címke, mint az 5200 v2-ben (`wave_events` v2), **assetenként külön** alkalmazva (saját σ, ATR, nem-átfedő szűrő), kiegészítve az első érintés idejével (`τ`), a közös CV-blokkokkal, az A-küszöb érzékenységi változataival és a prefixből (`[t0 − B, t_c]`) számolt order-flow jellemzőkkel. A riport a t931 benchmark és minden későbbi Scout-modell bemenete; a `methodology_doc` indoklása az 5300, a kód-referencia a `8330_reports.md`.

**Táblák:** `scout_events_v1, scout_events_v1_a125, scout_events_v1_a10` + `scout_flow_v1` a SOL lab DB-ben. A riport **csak train** adatot mutat: az OOS és a holdout-2 események csak darabszámként, osztály / címke nélkül szerepelnek (5300 holdout-2 szabály); a holdout-2 sorok címke-oszlopai a táblában NULL.

{{table:params}}

## 1. Darabszámok asset × split

Az OOS és a holdout-2 oszlop **csak darabszám**. A `purged` a split-határt átlógó, hiányos címke-ablakú, hiányos adatú, vagy a train előtti (SOL: 2020-09 → 2021-01) események. A BTC / ETH 1m adat 2021-01-01-től indul, így az első 7 nap σ nélkül nem ad eseményt (SOL: adat 2020-09-14-től, ezért 2021-01-01-től teljes).

{{table:splits}}

{{table:funnel}}

## 2. Train: asset × osztály × CV-blokk (W4h_H8h)

A blokkhatárok a három assetre **közösek** (naptári, egész napos). A `használható` esemény nem lóg át a blokkhatáron (az átlógók egyik foldba sem kerülnek). Az osztályok a train-eseményekre vonatkoznak.

{{chart:block_classes}}

{{table:blocks}}

A leíró W4h_H4h cella (csak összehasonlításra; nem elsődleges) train osztály-darabszámai:

{{table:h4_classes}}

## 3. Eseménygyakoriság és az impulzus-arány az A / σ_W függvényében

Események naponta (train, W4h_H8h): SOL 0.388; BTC 0.387; ETH 0.397. Az **A-küszöb érzékenység**: a lazább küszöb több eseményt ad, de a nem-átfedő szűrő miatt a halmazok nem egymás részhalmazai (külön táblák: `scout_events_v1`, `_a125`, `_a10`).

{{table:a_variants}}

{{chart:rate_a_sigma}}

{{table:rate_a_table}}

## 4. Minimum-ellenőrzés (5300 go/no-go 1. lépés)

Pooled train ≥ 1500 esemény és ≥ 300 impulzus; assetenként ≥ 300 és ≥ 50; CV-blokkonként ≥ 100 pooled használható teszt-esemény. Nem teljesülés: NO-GO (nem küszöb-csökkentés).

{{table:min_checks}}

## 5. Asset-közi egyidejűség (±4h, `t_c` szerint, train)

Az események aránya, amelynél egy **másik** asset eseménye is van ±4h-n belül. A „várt” oszlop a függetlenségi viszonyítás (Poisson, `1 − exp(−r·8h)`); a lift = megfigyelt / várt. A címke-egyezés oszlopok: a párok esetén az impulzus-arány aszerint, hogy a másik asset legközelebbi eseménye impulzus-e.

{{table:sim_asset}}

{{table:sim_pair}}

**Hatékony mintaméret.** A pooled train 2175 esemény 1076 különböző naptári napra esik (2.02 esemény / eseménynap), és 1337 idő-klaszterbe rendezhető (≤ 4h-n belüli láncolás; 1.63 esemény / klaszter). A nap-klaszter újramintavétel (5300) ennek megfelelően a napot tekinti egységnek.

## 6. Order-flow / mikrostruktúra jellemzők (prefix `[t0 − B, t_c]`)

A jellemzők csak a `t_c`-ig zárt 1 perces bar-okból épülnek; a baseline (volumen, kötésszám, quote-volume) a múltbeli, a `t0`-t megelőző 7 napból. **Szakaszok:** *decline* = `[t0 − B, t0]`, *rebound* = `(t0, t_c]`. `tb_ratio` = taker-buy / összvolumen (0,5 = kiegyensúlyozott); `tb_ratio_diff` = rebound − decline; `tb_ratio_slope_1h` az órás hányad OLS-meredeksége a prefixen; `delta_*_norm` = (taker-buy − taker-sell) / (7 napos átlagvolumen × percek); `trades_rel_*` = kötés/perc / 7 napos átlag; `trade_size_rel` = volumen/kötés a 7 napos arányhoz; `vol_*_rel` = átlagvolumen / 7 napos átlag; `vol_peak_rel`, `vol_peak_pos` = a legnagyobb 15m volumen relatív nagysága és helye a prefixen (0 = eleje, 1 = vége); `qv_logz_prefix` = a prefix log quote-volume összegének z-score-ja a megelőző 7 nap azonos hosszúságú ablakaihoz képest.

{{chart:flow_boxes}}

{{table:flow_by_class}}

**Leíró egyváltozós AUC** (impulzus vs a többi; 0,5 = nincs elválasztás; train, nincs szelekció, nem modell). Ez nem jellemző-kiválasztás: a modell-értékelés a t931 benchmark dolga, a skalár alapon felül.

{{table:flow_auc}}

## 7. Train példák assetenként és osztályonként

Osztályonként az idő szerinti középső train esemény (nincs válogatás). Kék: prefix (modell-bemenet, 15m, zár ≤ t_c); halvány: a címke-ablak (csak szemléltetés); alul a 15m taker-buy hányad a prefixen (szaggatott: t0).

{{chart:ex_sol}}

{{chart:ex_btc}}

{{chart:ex_eth}}

{{table:examples}}

## 8. Holdout-2 megvalósíthatóság (csak darabszám)

Holdout-2 (2026-08-01 →): az eseményszám **címke nélkül** számolható. A jelenlegi, teljesen kifutott címke-ablakú események (adat vége: 2026-11-21 10:29): **118** pooled esemény, **55** különböző napon. A 5300 minimuma: ≥ 250 esemény, ≥ 40 impulzus (a **train** impulzus-aránnyal, 14.2%), ≥ 60 különböző UTC nap, assetenként ≥ 50. Ráta-források: megfigyelt a holdout-2-ben, az utolsó 365 nap a holdout-2 előtt, és a train hosszú távú átlaga; a pesszimista becslés a legkisebbet, az optimista a legnagyobbat használja.

{{table:ho_assets}}

{{table:ho_criteria}}

## Értelmezés

**Minta (train, W4h_H8h):** pooled 2175 esemény, 308 impulzus (14.2%); SOL 720 esemény / 117 impulzus; BTC 718 esemény / 92 impulzus; ETH 737 esemény / 99 impulzus. A 5300 minimum-ellenőrzés **mind PASS**.

**Függőség:** az események 65.9%-ának van másik assetnél eseménye ±4h-n belül (a függetlenségi várt érték ennél jóval kisebb, lásd a lift oszlopot): a hatékony mintaméret az eseményszámnál kisebb (1337 idő-klaszter 2175 eseményre), ezért a nap-klaszter újramintavétel kötelező.

**Holdout-2:** jelenleg 118 esemény; a minimum becsült elérése 2027-04-26 (pesszimista, kötő: impulzusok (train arány 14.2%)) és 2027-04-07 (optimista, kötő: impulzusok (train arány 14.2%)). A minimum még **nem** teljesül: várakozás, nem küszöb-csökkentés.

**Order flow:** az osztályonkénti különbségek a fenti mediánokban és az egyváltozós AUC-ban láthatók; ezek leíró, train-only mutatók, a skalár alapon felüli többletet csak a t931 benchmark (azonos foldok, permutációs null) dönti el.
