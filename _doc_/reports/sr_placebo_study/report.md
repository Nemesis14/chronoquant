## Cél

**epic_071 / t913.** Hordoznak-e a detektált támasz/ellenállás szintek és trendvonalak *többlet* információt a véletlenhez képest? Osler (2000) módszerével minden valódi objektumhoz véletlen **placebo** objektumot készítünk azonos statisztikai szerkezettel, és ugyanazzal az esemény- és címke-logikával mérjük a touch → bounce, break → follow-through és retest → hold arányokat. Ha a valódi arány nem tér el a placebótól, a „szint” csak a volatilitást és az ár-dinamikát méri, nem egy valódi memóriát.

**Háttér:** módszertan és go/no-go küszöbök `_doc_/methodology_doc/1700_features_sr_trendline.md`; detektor `src/data_handling/sync_tables/_levels_polars.py` (config `levels_detector` **v1, befagyasztva**); bar-szintű feature-ök `feat_levels` (t912, `_levels_features.py`); a detektor vizuális riportja `sr_trendline_detection` (t911).

**Adat:** SOLUSDT `ohlcv` 1m 2020-09-14 → 2026-11-19 15:18 UTC (3,250,579 sor, read-only); a detektor a teljes történeten fut (bemelegedés), az események közül csak a **train = 2021-01-01 → 2025-05-01** és az **OOS = 2025-05-01 → adat vége** számít. ⚠️ **Az OOS időszakot a t911 riport vizuálisan megmutatta** (a user látta a 2025–2026-os ábrákat), ezért ez „vizuálisan látott”, nem teljesen tiszta OOS; a detektor paraméterei viszont a t911 után nem változtak. Az OOS határ az epic döntése (2025-05-01, a champion korábbi valid kezdete); a `config/models.json` szerint a jelenlegi champion train 2021-01 → 2026-01, valid 2026-02 → 2026-07, vagyis a champion a 2025-05 → 2026-01 szakaszon tanult — a feature-diagnosztika OOS oszlopa ezért nem a champion OOS-a.

Generálás: `uv run python -m reports.builders.sr_placebo` (~8 perc).

## Módszer

**Placebo objektumok** (minden valódi objektumhoz készletenként egy; a készletek determinisztikus seed-ből):

- *Placebo szint:* ugyanaz a lépték, ugyanaz a megerősítő bar (`confirm_idx`) és élettartam (`expired_at`), mint a valódi szinté; az ár `close[confirm] ± |d|·ATR[confirm]`, ahol `|d|` a lépték valódi szintjeinek megerősítéskori ATR-távolság-eloszlásából húzott, az előjel véletlen (1700: „az irány véletlen”).
- *Placebo vonal:* ugyanaz a típus (támasz/ellenállás), megerősítő bar, elévülés és elévülési ok; a vonal ára a megerősítő baron `close + d·ATR` (előjeles `d` az azonos típusú valódi vonalakból), a meredekség (ATR/bar) függetlenül húzva az azonos típusú valódi vonalak meredekség-eloszlásából.

**Azonos esemény- és címke-logika:** a placebo objektumokon a detektor állapotgépe (`_scan_object`: touch re-arm, break + szerepcsere, 12 bar retest-ablak, vonal halála a törés után) és a `forward_label_events` címkéi futnak. A sebesség miatt a builder ezeket vektorizálva futtatja a készleteken (`scan_batch`, `label_batch`); **minden futásban** a 0. placebo készletet a detektor saját függvényein is átküldi, és eseményre és címkére pontos egyezést követel (lásd az ellenőrző táblát). A detektor könyvtár nem változott.

**Cellák:** lépték (s/m/l) × objektum (szint/vonal) × irány (az objektum szerepe az eseménykor: támasz/ellenállás) × eseménytípus (touch/break/retest) = 36 cella. Pozitív címke: bounce, follow-through, hold. A jövőbe nem látó (null címkéjű) események kimaradnak.

**Statisztika:** Δ = valódi arány − placebo arány (a 1000 készleten összevonva). *Blokk-bootstrap:* 5000 ismétlés, blokk = 1 UTC nap; a napokat periódusonként együtt húzzuk újra az összes cellára (a valódi és a placebo esemény ugyanazon a napon párban marad). p = kétoldalas, a bootstrap SE-vel normál közelítéssel (az empirikus bootstrap p felbontása 1/ismétlésszám, ez a Holm-küszöbhöz kevés); *placebo-rang p* = a valódi arány helye a 1000 placebo-készlet arányának eloszlásában (Osler-féle, kiegészítő). **Holm-korrekció** a 36 cellán, periódusonként.

**Go/no-go (1700):** GO, ha (1) n ≥ 100 a train *és* az OOS időszakban is (feltételezés: a „cellánként” mindkét periódusra vonatkozik), (2) Holm p (train) < 0,01, (3) Δ train ≥ +5 pp, (4) az OOS Δ előjele megegyezik a trainével. Kiegészítő jelölés: **NO-GO (fordított)**, ha az 1, 2 és 4 teljesül, de Δ ≤ −5 pp (a hatás a hipotézissel ellentétes, de feature-ként informatív lehet).

{{table:verification}}

### A placebo eloszlások egyezése

A placebo objektumok a valódi távolság- és meredekség-eloszlást kvantilisekben reprodukálják (1700 validációs checklist). A szint előjeles távolsága szándékosan szimmetrizált (véletlen irány).

{{table:placebo_check}}

## 1. Eredmény cellánként (go/no-go)

**0 / 36 cella GO**, 0 fordított; értékelhető (n ≥ 100 mindkét periódusban): 21 cella. Az arányok a pozitív címke aránya (bounce / follow-through / hold); Δ százalékpontban.

{{table:go_nogo}}

{{chart:delta_forest}}

{{chart:train_vs_oos}}

### Placebo-eloszlás vs valódi arány

Cellánként a placebo készletek arányának hisztogramja (szürke = train, kék = OOS) és a valódi arány (piros folytonos = train, sárga pontozott = OOS). Ha a valódi vonal a hisztogram belsejébe esik, a szint nem különbözik egy véletlen szinttől.

{{chart:placebo_hist_s}}

{{chart:placebo_hist_m}}

{{chart:placebo_hist_l}}

## 2. Memória-görbe

A touch → bounce hatás (Δ = valódi − placebo) a szint/vonal kora szerint (kor = esemény ideje − az első érintés pivot-ideje, vonalnál az első horgonyé; a placebo ikre ugyanazt a kort kapja). Irányok összevonva. Üres marker: n < 100. Ha egy szintnek van „memóriája”, a friss szinteknél nagyobb Δ-t várnánk, ami a korral csökken.

{{chart:memory_curve}}

{{table:memory}}

## 3. Kiegészítő feature-diagnosztika (`feat_levels`)

A challenger tábla (`quant_train__solusdt_fw60_sr`) óránkénti (perc = 0) sorai, train 37,944 sor (a fw60 target így nem fed át). Minden `feat_levels` oszlopra: Spearman ρ a champion targetekkel (`long_mfe_fw60`, `short_mfe_fw60`) a nem-NULL sorokon, decilis-terjedelem (felső − alsó decilis target-átlaga bp-ben; kevés értékű oszlopnál a legnagyobb − legkisebb érték csoportja), OOS ρ az előjel-stabilitáshoz, és a redundancia: a legnagyobb |ρ| a 158 champion feature bármelyikével. Benchmark: a champion feature-ök saját |ρ|-eloszlása ugyanezen a mintán.

{{table:champion_benchmark}}

{{chart:feat_rho}}

{{chart:feat_redundancy}}

{{chart:feat_deciles}}

{{table:feat_diag}}

## Értelmezés

**Placebo-teszt.** A 36 cellából 0 GO és 0 fordított. A train |Δ| medián 1.5 pp, maximum 10.8 pp; a legalább 100 eseményes cellákban a legkisebb Holm p (train) 1.000. A placebo-rang p alapján a valódi arány a cellák 0 esetében esik a 1000 placebo-készlet eloszlásának szélső 5%-ába (korrigálatlanul, 36 cellánál véletlenül ~2 várható).

**Memória-görbe (szintek, train, n ≥ 100):** `s` <1d: 0.9 pp, `s` 1–7d: 0.1 pp, `s` 7–30d: -0.2 pp, `m` 1–7d: 1.1 pp, `m` 7–30d: -0.5 pp, `m` 30–180d: -1.2 pp, `l` 30–180d: -4.3 pp.
Korrigálatlanul p < 0,01 sávok: `s` vonal 7–30d: train 4.5 pp (korrigálatlan p 0.007, Holm p 0.08), OOS 5.5 pp; `m` vonal 30–180d: train 9.8 pp (korrigálatlan p 0.006, Holm p 0.07), OOS -4.6 pp. A Holm-korrekció után egyik sem szignifikáns, ezért ezek legfeljebb hipotézisek.

**Feature-diagnosztika.** A champion feature-ök medián |ρ|-ja 0.022 (max 0.351); a legerősebb feat_levels oszlopok: `feat_sr_dist_sup_atr_l` 0.131 (redundancia 0.32), `feat_sr_touches_res_l` 0.097 (redundancia 0.25), `feat_sr_touches_sup_l` 0.097 (redundancia 0.25), `feat_tl_dist_sup_atr_m` 0.089 (redundancia 0.46), `feat_sr_dist_sup_atr_m` 0.075 (redundancia 0.20). A szűrőn (|ρ| ≥ champion medián, előjel-stabil, redundancia < 0.8) 25 oszlop megy át: `feat_tl_dist_sup_atr_m`, `feat_sr_dist_sup_atr_m`, `feat_tl_dist_res_atr_s`, `feat_sr_touches_sup_m`, `feat_tl_dist_res_atr_m`, `feat_sr_n_active_m`, `feat_tl_break_age_m`, `feat_sr_dist_sup_atr_s`, `feat_sr_touches_res_m`, `feat_sr_is_round_res_s`, `feat_sr_touches_sup_s`, `feat_sr_n_active_s`, `feat_sr_touches_res_s`, `feat_tl_dist_sup_atr_s`, `feat_tl_retest_flag_m`.

## Döntési szempontok

- **A 1700 szabálya szerint a teljes detektor NO-GO:** egyetlen cellában sem nagyobb a valódi szintek és vonalak bounce / follow-through / hold aránya a placebo objektumokénál. A detektált struktúra eseményszinten nem hordoz a véletlen szinteken túli információt (SOLUSDT, v1 paraméterek).
- **⛔ DÖNTÉSI PONT (user):** (A) **megállás** — a t914 nem fut, az epic negatív eredménnyel zárul (a 1700 szerinti alapértelmezés); (B) **t914 exploratív futása** a teljes 50 feature-rel, mert az eseménytanulmány csak a címkézett események *irányított* hatását méri, a bar-szintű feature-ök (pl. távolság, erő, aktív szintszám) nemlineárisan vagy interakcióban még adhatnak jelet — ezt csak a tiszta A/B mutatná meg, de a várható haszon a fenti eredmény alapján kicsi; (C) **szűkített t914** csak a feature-diagnosztika szűrőjén átment oszlopokkal (kisebb zaj, gyorsabb).
- **Elemzői javaslat: (C), kifejezetten exploratív címkével.** A szűrőn átment 25 oszlop (l lépték nélkül): `feat_tl_dist_sup_atr_m`, `feat_sr_dist_sup_atr_m`, `feat_tl_dist_res_atr_s`, `feat_sr_touches_sup_m`, `feat_tl_dist_res_atr_m`, `feat_sr_n_active_m`, `feat_tl_break_age_m`, `feat_sr_dist_sup_atr_s`, `feat_sr_touches_res_m`, `feat_sr_is_round_res_s`, `feat_sr_touches_sup_s`, `feat_sr_n_active_s`, `feat_sr_touches_res_s`, `feat_tl_dist_sup_atr_s`, `feat_tl_retest_flag_m`, `feat_sr_dist_res_atr_s`, `feat_sr_dist_res_atr_m`, `feat_sr_retest_flag_s`, `feat_tl_break_age_s`, `feat_sr_flips_sup_s`, `feat_sr_strength_sup_s`, `feat_sr_flips_res_s`, `feat_sr_retest_flag_m`, `feat_tl_break_dir_s`, `feat_sr_is_round_res_m`. Ezek ρ-ja a long és a short targettel ellentétes előjelű (irány/pozíció-információ, nem volatilitás), és a legközelebbi champion társuk jellemzően a `feat_weekly_open_return` / előző session távolság — vagyis inkább „hol van az ár a közelmúlt tartományában” jellegű információt hordoznak, mint S/R-memóriát. A t914 akkor indokolt, ha a user ezt a gyengébb hipotézist is tesztelni akarja; az `l` lépték kimarad (kevés esemény, OOS-ben előjelet váltó ρ).
- Az OOS „vizuálisan látott”; a döntés a train Holm p-n és a train/OOS előjel-egyezésen alapul.
- Korlátok: a placebo nem rezsim-stratifikált; az események nincsenek dedupolva (a blokk-bootstrap kezeli az autokorrelációt); a léptékek közti duplikáció cellánként külön értékelt; a retest → hold definíció szigorú (1700 feltételezés).
