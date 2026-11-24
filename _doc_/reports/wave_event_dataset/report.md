## Cél

**Ellenőrzőpont (epic_072 / t925, v2):** a t924 módszertan (`_doc_/methodology_doc/5200_wave_start_image_model.md`, v2) szerint újraépített eseményadatkészlet. A t919 (kép-embedding, klaszterezés) előtt számokkal és képekkel ellenőrizzük: elég esemény van-e cellánként és CV-blokkonként, mekkora a három címke-osztály aránya, mennyire számít a same-bar kétértelműség, és hogyan függ az impulzus-arány az `A/σ_W`-től.

**Adat:** SOLUSDT `ohlcv` 1m, 2020-09-14 → adat vége, 3,251,811 sor, read-only. Adatkészlet: nem íródott a lab DB-be (`--no-store`). Újragenerálás: `uv run python -m reports.builders.wave_event_dataset` (v1: `--version v1`).

> ⚠️ **OOS-tisztaság:** minden példa, kép, eloszlás és címke-arány **kizárólag a train** időszakból (2021-01-01 → 2026-01-31) származik. Az OOS és a holdout-2 itt kizárólag **darabszámként** szerepel (nincs példa, nincs címke-arány).

{{table:params}}

## 1. Eseményszám és osztályarányok cellánként

Rács: csak `W = 4h`, `H ∈ {4h, 8h}`. A (4h,4h) és (4h,8h) esemény-halmaz nagyrészt közös `t0`-kból áll, de a nem-átfedő greedy cellánként külön fut, ezért a két halmaz nem azonos. Az OOS / holdout-2 oszlop csak darabszám.

{{table:summary}}

### Tölcsér: jelölt → elfogadott

Jelölt = a `[t0 − B, t_c]` ablak minimuma; A-szűrő = `A ≥ 1,5·σ_W`; elfogadott = a teljes-intervallumos nem-átfedő greedy után (az OOS / holdout-2 oszlop csak darabszám).

{{table:funnel}}

## 2. Same-bar kettős érintés (train)

Ha ugyanazon az 1 perces baron egyszerre `high > P*` és `low < low[t0]`, a bar belső sorrendje ismeretlen; a szabály szerint az esemény **hamis indulás** (konzervatív). Az alábbi tábla a gyakoriságot és — érzékenységként — az osztályarányokat az ellenkező tie-break (same-bar = impulzus) mellett mutatja; lényeges, ha > 2%.

{{table:same_bar}}

## 3. CV-blokkok (train, 6 időblokk)

A blokk-határok egész napok, közel egyenlő hosszal: 2021-01-01, 2021-11-07, 2022-09-12, 2023-07-18, 2024-05-23, 2025-03-29 (az utolsó blokk a train-határig, 2026-01-31-ig tart). Egy esemény blokkja a `t0` blokkja; ha a teljes `[t0 − B, t_c + H]` intervalluma kilóg a blokkjából, **átlógó** (egy foldba sem kerül). Az alábbi darabszámok a használható teszt-eseményeket mutatják. A purge (átfedő intervallum) és az embargo (`H` a teszt-blokk szélein) fold-időben, a `cv_fold` segédfüggvénnyel alkalmazandó (tiszta függvény, tesztelve).

{{table:cv_blocks}}

{{chart:cv_block_shares}}

### 5200 minimumok (CV): blokkonként ≥ 40 teszt-esemény; összevontan ≥ 100 impulzus és ≥ 100 nem-impulzus

A „Train min/max” a fold-tanítóhalmaz mérete purge + embargo és az átlógók kizárása után.

{{table:cv_checks}}

## 4. A/σ_W és impulzus-arány (train)

Mivel a `P* = close[t_c]·exp(A)` az `A`-val együtt távolodik, nagy visszapattanás után nehezebb az impulzus. Ez a skalár alap (S0) legerősebb nyers tagja, a kép-modellnek ezt kell felülmúlnia. Az impulzus-arány kvintilisenként: W4h_H4h: Q1 15.4% → Q5 1.9%; W4h_H8h: Q1 28.5% → Q5 2.8%. Nyers leírás (nincs placebo / CV), nem teszt.

{{chart:quint_impulse}}

{{table:quintiles}}

{{chart:dist_a_sigma}}

## 5. OOS és holdout-2: csak darabszám

Az OOS-ellenőrzés feltétele (5200 v2): ≥ 100 esemény a két 4h-s cellában együtt. A két cella `t0`-i nagyrészt közösek, ezért a **cella-összeg duplán számol**; a konzervatív mérték az **egyedi `t0`-k száma**. Címke-arány vagy példa az OOS-ból nem szerepel.

{{table:oos_counts}}

## 6. Példák: a modell-bemenet (prefix-kép) osztályonként (train)

**Kiválasztás (determinisztikus):** cellánként és osztályonként a train események időrendben, a (k + 0,5)/3 pozíciókban. A prefix-képek 15m OHLC-k a `low[t0]`-hoz viszonyítva, ATR14(15m)-mel normálva, alattuk a relatív volumen; a modell nem lát naptári időt, árszintet vagy `P*`-ot. Alatta osztályonként egy kinagyított, címkézett példa (kék = prefix, halvány = a H címke-ablak; ★ = P* első átlépése, ✕ = low[t0] első törése).

### Cella (W=4h, H=4h)

{{chart:input_w4h_h4h_impulse}}

{{chart:input_w4h_h4h_false_start}}

{{chart:input_w4h_h4h_sideways}}

**W4h_H4h — impulzus:** t0 2023-09-13 17:48, t_c 2023-09-13 21:48 UTC; A = 2.80% (1.52 σ_W), MFE +5.18%, MAE -0.23%.

{{chart:ex_w4h_h4h_impulse}}

**W4h_H4h — hamis indulás:** t0 2023-04-03 15:01, t_c 2023-04-03 19:01 UTC; A = 3.26% (2.22 σ_W), MFE +1.34%, MAE -3.29%.

{{chart:ex_w4h_h4h_false_start}}

**W4h_H4h — oldalazás:** t0 2023-07-14 20:34, t_c 2023-07-15 00:34 UTC; A = 7.52% (2.33 σ_W), MFE +3.10%, MAE -0.30%.

{{chart:ex_w4h_h4h_sideways}}

### Cella (W=4h, H=8h)

{{chart:input_w4h_h8h_impulse}}

{{chart:input_w4h_h8h_false_start}}

{{chart:input_w4h_h8h_sideways}}

**W4h_H8h — impulzus:** t0 2023-06-20 14:30, t_c 2023-06-20 18:30 UTC; A = 4.31% (2.54 σ_W), MFE +4.93%, MAE -1.04%.

{{chart:ex_w4h_h8h_impulse}}

**W4h_H8h — hamis indulás:** t0 2023-03-07 22:09, t_c 2023-03-08 02:09 UTC; A = 2.32% (1.56 σ_W), MFE +0.18%, MAE -5.22%.

{{chart:ex_w4h_h8h_false_start}}

**W4h_H8h — oldalazás:** t0 2023-08-31 21:09, t_c 2023-09-01 01:09 UTC; A = 2.09% (1.55 σ_W), MFE +0.54%, MAE -0.51%.

{{chart:ex_w4h_h8h_sideways}}

{{table:examples}}

## Értelmezés

**Osztályarányok (train):** W4h_H4h: 779 train, impulzus 8.5% / hamis indulás 7.4% / oldalazás 84.1%; W4h_H8h: 720 train, impulzus 16.2% / hamis indulás 16.0% / oldalazás 67.8%. **Same-bar:** W4h_H4h 0.00%; W4h_H8h 0.00% (lényegességi küszöb 2%).

**CV-minimumok:** W4h_H4h: FAIL (min. blokk 118, impulzus 66, nem-impulzus 712); W4h_H8h: PASS (min. blokk 108, impulzus 117, nem-impulzus 601).

**OOS:** 143 esemény a két cellában összesen (cella-összeg), **75 egyedi `t0`**; a ≥ 100 feltétel az egyedi számon NEM teljesül. A holdout-2 érintetlen (csak darabszám).

**Állásfoglalás:** az ellenőrzőpont a t919 előtt user döntés: a CV-minimumok és az OOS-szám fent közölt eredménye adja meg, mely cella értékelhető. A minimum alatti cella a 5200 szerint NO-GO (nem küszöb-csökkentés).
