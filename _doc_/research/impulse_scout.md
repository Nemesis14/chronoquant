# Impulse Scout — több-asset impulzus-előrejelző modellcsalád (epic_073, lezáró összefoglaló)

**Időszak:** 2026-11-21 – 22 · **Státusz:** lezárt, nemleges (a skalár S0 + C baseline marad) · **Módszertan:** `_doc_/methodology_doc/5300_impulse_scout.md` (v1) · **Riport:** `_doc_/reports/scout_leaderboard/` (+ `scout_events`, `scout_lgbm`, `scout_rocket`, `scout_hazard`, `scout_foundation`)

## Kérdés

Egy megerősített lokális mélypont után (`t_c = t0 + 4h`) a következő 8 órában impulzus (P* előbb), hamis indulás (`low[t0]` előbb) vagy oldalazás jön-e. Két kérdés: (1) SOL + BTC + ETH összevonásával van-e olyan modellcsalád, amely a skalár S0 + C logitnál (epic_072) zajtól megkülönböztethetően jobban rangsorolja az impulzust; (2) díjak után marad-e kereskedhető él, akár a modellből, akár a kereskedési szabályból.

## Adat

- t930 `research."scout_events_v1"` (a 5200 v2 esemény- és címkeszabályai assetenként, A ≥ 1,5 σ_W, W4h_H8h): train (2021-01 → 2026-01) **2175 esemény / 308 impulzus** (14,2%), ebből a CV-ben 2170; SOL 720 / 117, BTC 718 / 92, ETH 737 / 99. Az 5300 minimumai teljesültek (az impulzusoké szűken: 308 vs 300).
- Erős asset-közi egyidejűség: az események 66%-ánál ±4 órán belül egy másik assetnek is van eseménye, ezért 2175 esemény csak ~1340 független idő-klaszter. Emiatt minden újramintavétel napi klaszterekkel történt.
- Order flow: 20 jellemző (`scout_flow_v1`), egyváltozósan gyenge (AUC 0,44–0,54).
- OOS (2026-02 → 07) nem vett részt; holdout-2 (2026-08-01 →) nem töltődött be.

## Protokoll

6 közös naptári blokkos purged CV, a teljes esemény-intervallum purge-olva, 8 órás embargóval, minden modellnek ugyanazok a foldok. Metrikák: ΔAUC nap-klaszter bootstrap CI-vel, 3 osztályos log-loss, felső 20% / 10% precizitás (teszt-foldon belüli rangsor), blokk- és asset-stabilitás, triple-barrier szimuláció a `config/strategies.json` költségeivel (maker 2 / taker 5 / csúszás 1,5 bp). Többszörös tesztelés: Holm a három előre regisztrált elsődlegesre (H1 LightGBM, H2 MiniRocket, H3 hazard; nap-csomagos permutáció, N = 1000, teljes refit). Hansen SPA a teljes leaderboardra. Az SPA-kapu csak a kiválasztott legjobb modellre vonatkozik, a gazdasági kapu a felső 20%-ot és a 10%-ot is nézi.

## 1. kör: modellcsaládok (előre rögzített konfigurációk)

| Modell | Szerep | AUC | ΔAUC [95% CI] | 3-oszt. ΔLL | Top 20% prec. | perm. p | Holm (H1–H3) | Nettó bp, top 20% [CI] | Döntés |
|---|---|---|---|---|---|---|---|---|---|
| S0 + C logit (baseline) | mérce | 0,636 | — | — | 0,241 | 0,001 (AUC) | — | +2,7 [−14,7; +21,4] | marad |
| LightGBM, 50 jellemző + flow | H1 | 0,623 | −0,013 [−0,046; +0,023] | −0,025 | 0,221 | 0,720 | 0,806 | −6,8 [−30,6; +16,9] | NO-GO |
| MiniRocket, nyers prefix | H2 | 0,632 | −0,004 [−0,035; +0,030] | −0,023 | 0,216 | 0,306 | 0,806 | −17,8 [−47,1; +9,7] | NO-GO |
| Diszkrét idejű hazard | H3 | 0,647 | +0,011 [−0,004; +0,027] | −0,033 | 0,228 | 0,269 | 0,806 | −8,9 [−28,1; +10,3] | NO-GO |
| InceptionTime | kísérleti | 0,627 | −0,009 [−0,042; +0,025] | −0,013 | 0,234 | nem futott | — | +0,1 [−23,1; +24,3] | NO-GO |
| Chronos-Bolt embedding | kísérleti | 0,625 | −0,011 [−0,047; +0,026] | +0,007 | 0,202 | 0,692 (N=200) | — | −2,1 [−25,7; +21,7] | NO-GO |
| Chronos + S0C | kísérleti | 0,640 | +0,004 [−0,030; +0,037] | −0,001 | 0,216 | 0,498 (N=200) | — | −0,0 [−24,7; +25,0] | NO-GO |

- **Univerzum SPA** (6 modell a baseline ellen, bináris log-loss, napi összeg): legjobb a hazard modell, **p = 0,23**. Egyik modell sem jelölt.
- Mind a 7 OOF újraszámolva, a tárolt AUC-kal bitre egyezik. A futásonkénti korábbi SPA-értékek (0,021 / 0,013 / 0,091) elavultak: az első kettő egy azóta javított `>` hiba eredménye volt, a hazardé egymodelles volt.

## 2. kör: kereskedési szabályok a baseline score-on (user döntés, 2026-11-22)

A user döntése szerint a 2. kör nem új modelltípus. 12 előre regisztrált szabályváltozatot teszteltünk a baseline OOF score-on, mindet a futtatás előtt rögzítettük a t936 ticketben: σ_W- és ATR-többszörös TP/SL, részleges kilépés break-even stoppal, break-even trigger, 4 és 2 órás időkorlát, A-sáv szűrő, felső 30%, felső 10% + aszimmetrikus barrier. Stratégia-SPA 26 stratégián: a 7 modell × felső 20% / 10% az alap szabállyal, plusz a 12 változat.

| Változat | Jelzés | Nettó bp / jelzés [95% CI] | Hit / break-even | Blokk > 0 | Asset > 0 |
|---|---|---|---|---|---|
| Referencia: alap (P* / low[t0] / 8h) | top 20% | +2,7 [−14,7; +21,4] | 24,1% / 23,3% | 3/6 | 1/3 |
| **R2_03: TP +3 σ_W / SL −1,5 σ_W** (kiválasztott) | top 20% | **+15,5 [−5,2; +37,7]** | 9,2% / 5,8% | 4/6 | 2/3 |
| R2_10: alap + A/σ_W < 2,5 | top 20% | +2,0 [−16,8; +21,0] | 24,1% / 23,4% | 3/6 | 1/3 |
| R2_07: +1 σ_W után break-even | top 20% | +1,1 [−16,2; +18,9] | 22,8% / 22,4% | 3/6 | 1/3 |
| R2_08: időkorlát 4h | top 20% | −1,0 [−15,7; +15,2] | 14,7% / 15,1% | 3/6 | 1/3 |
| R2_02: TP +2 σ_W / SL −1 σ_W | top 20% | −1,0 [−17,0; +16,8] | 16,6% / 16,9% | 2/6 | 1/3 |
| R2_06: 50% félúton + break-even | top 20% | −2,0 [−17,3; +13,3] | 21,6% / 22,5% | 2/6 | 1/3 |
| R2_01: ±1 σ_W | top 20% | −4,1 [−16,5; +10,3] | 40,5% / 42,2% | 3/6 | 1/3 |
| R2_11: alap | top 30% | −7,8 [−23,7; +8,4] | 21,6% / 24,0% | 1/6 | 1/3 |
| R2_12: TP +2 σ_W / SL −1 σ_W | top 10% | −8,5 [−33,1; +15,6] | 15,6% / 18,2% | 2/6 | 1/3 |
| R2_05: TP +4 ATR / SL −2 ATR | top 20% | −9,7 [−23,9; +5,2] | 22,8% / 26,2% | 2/6 | 1/3 |
| R2_04: TP +2 ATR / SL −1 ATR | top 20% | −10,9 [−19,2; −1,8] | 31,7% / 38,3% | 0/6 | 0/3 |
| R2_09: időkorlát 2h | top 20% | −14,0 [−24,9; −2,6] | 6,7% / 12,3% | 2/6 | 1/3 |

- **Stratégia-SPA a „nincs kereskedés” benchmark ellen:** a legjobb az R2_03, **p = 0,44**. A kiválasztott változat a CI-feltételen (alsó határ −5,2 bp) és az SPA-n is elbukik, ezért **NO-GO**.
- A baseline alap szabálya ellen az R2_03 SPA p = 0,023. A szélesebb cél + szűkebb stop tehát a referencia szabálynál valószínűleg jobb, de abszolút, díjak utáni nyereség nem igazolt.
- Az R2_03 nyeresége koncentrált: SOL +69 bp, BTC +6 bp, ETH −9 bp; a B5–B6 blokkban negatív. A hit-arány 9%, az eredményt kevés nagy nyerő hordozza.

## Konklúzió

- **Hol van a jel:** a skalár S0 + C (főleg a visszapattanás relatív mérete, A/σ_W) valódi, de szerény rangsoroló jelet hordoz (AUC 0,636, permutációs p 0,001). Erre a jelre a vizsgált modellcsaládok egyike sem tudott zajtól megkülönböztethető többletet tenni.
- **Mi bukott el és miért:** a gazdagabb modellek (fa, sorozat-konvolúció, hazard, előtanított embedding) a 3 osztályos valószínűséget javították: a hamis indulás és az oldalazás arányát a baseline állandónak veszi, ők becsülik. Az impulzus-rangsort viszont nem javították. 308 impulzusnál a flexibilis modellek túlilleszkednek (kalibrációs meredekség 0,46–0,53), a további jellemzők (order flow, alak, BTC-kontextus, embedding) pedig az A/σ_W-vel átfedő információt hordoznak. Gazdaságilag a szelekció AUC-többlete nem válik díjak utáni hozammá: a felső 20% szelekciónál a hit-arány alig van a break-even felett.
- **A kereskedési szabály számít, de nem elég:** a szélesebb cél / szűkebb stop geometria a legjobb irány (+15,5 bp), de a CI-je a nullát tartalmazza, az SPA nem szignifikáns, és az eredmény a SOL-ra koncentrálódik.
- **Döntés:** a Scout a skalár S0 + C logit marad, döntési forrásként nem minősíthető. A negatív eredmény érvényes, a protokoll szerint dokumentált.

## Holdout-2 terv

- **Legkorábbi időpont:** a t930 ráta-becslése szerint a kötő feltétel a ≥ 40 impulzus: optimista 2027-04-07, pesszimista **2027-04-26**. A ≥ 250 pooled esemény 2027-03-11 / 03-26-ra, a 60 nap 2026-12-01-re, az assetenkénti 50 esemény 2026-12-26-ra teljesül. A teljes feltételkészlet így legkorábban **2027 április végén** áll elő; ezt címke nélküli esemény-számolással kell ellenőrizni.
- **Előre regisztrált jelölt (a t936 ticket rögzített szabálya szerint):** a 2. kör kiválasztott változata (R2_03) nem teljesítette a feltételeket. A holdout-2 referencia-próbája ezért az **S0 + C baseline + az alap triple-barrier szabály (P* / low[t0] / 8h, felső 20%)**, befagyasztott foldok és küszöb mellett.
- **Státusz:** GO- vagy kísérleti jelölt nincs, ezért az 5300 szerint a holdout-2 megnyitása GO-próbaként nem javasolt. Egy leíró, megerősítő próba (a baseline jel valódisága és előjele holdouton) csak a user külön döntésével indulhat, a fenti időpont után, egyszer. Az R2_03 holdouton nem tesztelhető jelöltként; ha a jövőben ez az irány folytatódik, új verzió (v2) és új, érintetlen ablak kell hozzá.

## Korlátok

- A funding nincs modellezve; a same-bar konvenció konzervatív.
- Az InceptionTime permutációja nem futott, a Chronos-é csökkentett (N = 200).
- A 2. kör szabály-rácsa részben a t930 leíró ismereteire támaszkodott (A-sáv szűrő).
- A SOL OOS-t a t928 már látta, ezért itt nem szerepel.

## Újragenerálás

A lezáró futás a `reports.builders.scout_leaderboard` modul (`--stage oof | eval | report | refresh`, research uv csoport). Az OOF-ok a lab DB `research."scout_oof_v1"`, a stratégia-univerzum a `research."scout_universe_v1"` táblában van. A kód a repóban marad (`src/reports/builders/scout_*`, `scout_bench/`).
